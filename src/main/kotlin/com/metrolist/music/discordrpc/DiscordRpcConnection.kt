package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.Activity
import com.metrolist.music.discordrpc.entities.Assets
import com.metrolist.music.discordrpc.entities.Button
import com.metrolist.music.discordrpc.entities.Metadata
import com.metrolist.music.discordrpc.entities.Timestamps
import com.metrolist.music.discordrpc.entities.Presence
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import org.json.JSONObject
import timber.log.Timber
import java.util.concurrent.atomic.AtomicLong

enum class ActivityType(val value: Int) {
    PLAYING(0),
    STREAMING(1),
    LISTENING(2),
    WATCHING(3),
    COMPETING(5),
}

class DiscordRpcConnection(
    private val token: String,
    os: String = "Android",
    browser: String = "Discord Android",
    device: String = "Generic Android Device",
    private val userAgent: String = "Discord-Android/314013;RNA",
    private val superPropertiesBase64: String? = null,
    private val gatewayFactory: (String, String, String, String) -> GatewayWebSocket = { t, o, b, d ->
        GatewayWebSocket(t, o, b, d)
    },
    /**
     * Test seam: replaces the network external-assets upload with a pure fake so the
     * two-phase image resolution (item 4) can be exercised deterministically in unit tests
     * (the module stays MockK-free). Null in production.
     */
    private val externalAssetFetcher: (suspend (String) -> String?)? = null,
) {
    private val tag = "DiscordRpc"
    private val gateway = gatewayFactory(token, os, browser, device)
    private val httpClient = HttpClient()
    private val httpScope = CoroutineScope(SupervisorJob() + DiscordRpc.backgroundDispatcher + httpScopeExceptionHandler)
    private var lastUpdateTime = 0L
    private val minUpdateInterval = 500L // Minimum 500ms between updates

    /**
     * Monotonic id of the most recent activity write request. A [setActivity] call captures
     * its id on entry and re-checks it right before writing: a newer [setActivity] or
     * [clearActivity] (song skip, browsing clear) bumps the id, so the superseded call is
     * dropped instead of re-sending a stale activity after its image-resolution/network wait.
     */
    private val activityId = AtomicLong(0)

    /**
     * In-flight image-resolution job of the most recent [setActivity] (item 4, phase 2).
     * Cancelled by a newer setActivity with DIFFERENT images, by clearActivity and by
     * close/closeDirect, so a superseded upload can never re-send a stale presence or
     * write to the cache. A newer setActivity that re-sends the SAME images (the app's
     * 5 s refresh tick moves the timestamps on the same artwork) keeps the in-flight
     * upload running: cancelling it on every tick would kill a slow first upload before
     * it completes, leaving the presence image-less indefinitely while playback is
     * stable (regression pinned by DiscordRpcConnectionTwoPhaseTest).
     */
    private var imageResolutionJob: Job? = null

    /** Last images handed to [setActivity] — the cancel decision above compares against these. */
    private var lastLargeImage: String? = null
    private var lastSmallImage: String? = null

    /** True once the underlying gateway gave up retrying (see GatewayWebSocket.MAX_RECONNECT_ATTEMPTS). */
    val reconnectAbandoned: StateFlow<Boolean>
        get() = gateway.reconnectAbandoned

    /**
     * Terminal close code reported by the gateway (4004 invalid token), or null. The
     * gateway is private, so this proxy lets the app surface a durable error (item 9).
     */
    val terminalCloseCode: StateFlow<Int?>
        get() = gateway.terminalCloseCode

    /**
     * Empties the process-wide artwork cache (fork addition, item 10): the app calls this
     * on token change so a new account never inherits the previous account's uploaded
     * asset mappings.
     */
    fun clearArtworkCache() {
        ArtworkCache.clear()
    }

    fun isRunning(): Boolean = gateway.isSessionEstablished()

    fun connect() {
        Timber.tag(tag).i("connect() called")
        gateway.connect()
    }

    suspend fun setActivity(
        name: String?,
        type: ActivityType = ActivityType.LISTENING,
        state: String? = null,
        details: String? = null,
        timestamps: Timestamps? = null,
        largeImage: String? = null,
        largeText: String? = null,
        smallImage: String? = null,
        smallText: String? = null,
        buttons: List<Button>? = null,
        status: String = "online",
        since: Long? = null,
        applicationId: String? = null,
    ) {
        val expectedActivityId = activityId.incrementAndGet()
        val currentTime = System.currentTimeMillis()
        val elapsed = currentTime - lastUpdateTime
        if (elapsed < minUpdateInterval) {
            val delay = minUpdateInterval - elapsed
            Timber.tag(tag).d("setActivity: debouncing, waiting ${delay}ms (last update ${elapsed}ms ago)")
            delay(delay)
        }
        lastUpdateTime = System.currentTimeMillis()
        
        val startTime = lastUpdateTime
        Timber.tag(tag).i("setActivity: type=$type state=$state details=$details buttons=${buttons?.size}")
        Timber.tag(tag).d("setActivity: largeImage present=${largeImage != null}, smallImage present=${smallImage != null}")

        if (!isRunning()) {
            Timber.tag(tag).d("setActivity: gateway not running, connecting...")
            gateway.connect()
        }

        // Phase 1 (upstream parity: DiscordRpcManager.setActivity L345-359): send the
        // text-only presence IMMEDIATELY — the activity must never be blocked on image
        // resolution / the external-assets upload.
        sendPresence(
            name = name,
            type = type,
            state = state,
            details = details,
            timestamps = timestamps,
            resolvedLargeImage = null,
            largeText = largeText,
            resolvedSmallImage = null,
            smallText = smallText,
            buttons = buttons,
            status = status,
            since = since,
            applicationId = applicationId,
            expectedActivityId = expectedActivityId,
        )

        // Cancel the previous in-flight image job when the images actually changed: a
        // superseded upload must not re-send a stale presence. A no-image or different-image
        // supersede cancels; a same-image supersede (the app refreshes the timestamps on the
        // same artwork every tick) lets the in-flight upload finish so the cache can hold
        // the asset for the next write. The activityId re-check in the job stays the
        // backstop against a stale second send.
        if (largeImage != lastLargeImage || smallImage != lastSmallImage) {
            imageResolutionJob?.cancel()
        }
        lastLargeImage = largeImage
        lastSmallImage = smallImage

        val hasImages = !largeImage.isNullOrBlank() || !smallImage.isNullOrBlank()
        if (!hasImages) {
            // No images: exactly one send (upstream parity: RM L366 early return).
            Timber.tag(tag).i("setActivity completed in ${System.currentTimeMillis() - startTime}ms (no images)")
            return
        }

        // Phase 2 (upstream parity: RM L361-427): resolve the images in a cancellable job
        // and re-send with the mp: assets only if this write still won the race.
        imageResolutionJob = httpScope.launch {
            val resolvedLargeImage = largeImage?.let {
                Timber.tag(tag).v("Resolving large image: ${it.takeLast(50)}")
                resolveImage(it).also { result ->
                    Timber.tag(tag).v("Large image resolved: $result")
                }
            }
            val resolvedSmallImage = smallImage?.let {
                Timber.tag(tag).v("Resolving small image: ${it.takeLast(50)}")
                resolveImage(it).also { result ->
                    Timber.tag(tag).v("Small image resolved: $result")
                }
            }
            Timber.tag(tag).d("Image resolution took ${System.currentTimeMillis() - startTime}ms")

            // A newer setActivity/clearActivity bumped the id while the images were
            // resolving — the newer write wins, drop this stale second send.
            if (expectedActivityId != activityId.get()) {
                Timber.tag(tag).w("setActivity: superseded during image resolution (expected=$expectedActivityId current=${activityId.get()}), skipping")
                return@launch
            }

            sendPresence(
                name = name,
                type = type,
                state = state,
                details = details,
                timestamps = timestamps,
                resolvedLargeImage = resolvedLargeImage,
                largeText = largeText,
                resolvedSmallImage = resolvedSmallImage,
                smallText = smallText,
                buttons = buttons,
                status = status,
                since = since,
                applicationId = applicationId,
                expectedActivityId = expectedActivityId,
            )
        }
        Timber.tag(tag).i("setActivity phase 1 sent in ${System.currentTimeMillis() - startTime}ms (image resolution in flight)")
    }

    /**
     * Builds and sends one presence update — phase 1 (text-only) or phase 2 (with resolved
     * images) of [setActivity]. [resolvedLargeImage]/[resolvedSmallImage] are the already
     * resolved asset values (null in phase 1). The stale guard drops the write when a newer
     * setActivity/clearActivity superseded this one while waiting on the session.
     */
    private suspend fun sendPresence(
        name: String?,
        type: ActivityType,
        state: String?,
        details: String?,
        timestamps: Timestamps?,
        resolvedLargeImage: String?,
        largeText: String?,
        resolvedSmallImage: String?,
        smallText: String?,
        buttons: List<Button>?,
        status: String,
        since: Long?,
        applicationId: String?,
        expectedActivityId: Long,
    ) {
        val buttonLabels = buttons?.map { it.label }?.takeIf { it.isNotEmpty() }
        val buttonUrls = buttons?.map { it.url }?.takeIf { it.isNotEmpty() }

        Timber.tag(tag).d("Sending presence update to gateway (assets=${resolvedLargeImage != null || resolvedSmallImage != null})")
        gateway.updatePresence(
            Presence(
                activities = listOf(
                    Activity(
                        name = name ?: "Metrolist",
                        type = type.value,
                        applicationId = applicationId,
                        state = state,
                        details = details,
                        timestamps = timestamps,
                        assets = if (resolvedLargeImage != null || resolvedSmallImage != null) {
                            Assets(
                                largeImage = resolvedLargeImage,
                                largeText = largeText,
                                smallImage = resolvedSmallImage,
                                smallText = smallText,
                            )
                        } else null,
                        buttons = buttonLabels,
                        metadata = buttonUrls?.let { Metadata(buttonUrls = it) },
                    ),
                ),
                since = since,
                status = status,
                afk = false,
            ),
            staleCheck = { expectedActivityId == activityId.get() },
        )
    }

    suspend fun clearActivity(status: String = "online") {
        // Bump the id so any setActivity still in flight (image resolution, session wait)
        // is dropped when it resumes — a clear must always win over stale writes.
        activityId.incrementAndGet()
        // Kill the in-flight image job too: its second send must not resurrect the
        // activity after a clear.
        imageResolutionJob?.cancel()
        if (isRunning()) {
            Timber.tag(tag).i("Clearing activity")
            gateway.clearPresence()
        }
    }

    suspend fun close() {
        Timber.tag(tag).i("close() called")
        clearActivity()
        gateway.close()
        httpScope.cancel()
        httpClient.close()
    }

    fun closeDirect() {
        Timber.tag(tag).i("closeDirect() called")
        // No clearPresence here (session may be down), but still invalidate in-flight
        // writes and kill the in-flight image resolution.
        activityId.incrementAndGet()
        imageResolutionJob?.cancel()
        gateway.close()
        httpScope.cancel()
        httpClient.close()
    }

    private suspend fun resolveImage(image: String): String? {
        if (image.isBlank()) return null
        return if (image.startsWith("mp:") || image.startsWith("http")) {
            ArtworkCache.getOrFetch(image) {
                if (image.startsWith("mp:")) {
                    Timber.tag(tag).d("Image already mp: — $image")
                    image
                } else {
                    val fetcher = externalAssetFetcher
                    // The seam must distinguish "no fetcher" from "fetcher returned null"
                    // (a failed upload): an elvis fallback would route a null seam result
                    // to the real network path. The real upload runs directly in this
                    // resolution job (no sibling scope), so a cancelled resolution cancels
                    // the HTTP request as well.
                    val asset = if (fetcher != null) {
                        fetcher(image)
                    } else {
                        Timber.tag(tag).d("Fetching external asset for: $image")
                        fetchExternalAsset(
                            client = httpClient,
                            applicationId = DiscordRpc.APPLICATION_ID,
                            token = token,
                            imageUrl = image,
                            userAgent = userAgent,
                            superPropertiesBase64 = superPropertiesBase64,
                        )
                    }
                    if (asset != null) {
                        Timber.tag(tag).i("External asset uploaded: $image -> $asset")
                    } else {
                        Timber.tag(tag).w("External asset upload failed for: $image, using raw URL")
                    }
                    asset ?: image
                }
            }
        } else {
            "mp:$image"
        }
    }

    companion object {
        private const val TAG = "DiscordRpc"

        internal val httpScopeExceptionHandler = CoroutineExceptionHandler { _, e ->
            Timber.tag("DiscordRpc").e(e, "httpScope coroutine failed")
        }

        suspend fun getUserInfo(
            token: String,
            userAgent: String = SuperProperties.userAgent,
            superPropertiesBase64: String? = null,
        ): Result<UserInfo> = runCatching {
            Timber.tag(TAG).i("Fetching user info from Discord API...")
            val client = HttpClient()
            try {
                val response = client.get("https://discord.com/api/v9/users/@me") {
                    header("Authorization", token)
                    header("User-Agent", userAgent)
                    if (superPropertiesBase64 != null) {
                        header("X-Super-Properties", superPropertiesBase64)
                    }
                }
                val text = response.bodyAsText()
                val json = JSONObject(text)
                val id = json.getString("id")
                val username = json.getString("username")
                val name = json.optString("global_name", username)
                val avatarHash = json.optString("avatar")
                val avatar = if (avatarHash.isNotEmpty() && avatarHash != "null") {
                    "https://cdn.discordapp.com/avatars/$id/$avatarHash.png"
                } else null
                Timber.tag(TAG).i("User info fetched successfully")
                UserInfo(id, username, name, avatar)
            } finally {
                client.close()
            }
        }
    }
}
