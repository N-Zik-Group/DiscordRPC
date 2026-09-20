package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.ClientState
import com.metrolist.music.discordrpc.entities.HeartbeatResponse
import com.metrolist.music.discordrpc.entities.Identify
import com.metrolist.music.discordrpc.entities.IdentifyProperties
import com.metrolist.music.discordrpc.entities.OpCode
import com.metrolist.music.discordrpc.entities.Payload
import com.metrolist.music.discordrpc.entities.Presence
import com.metrolist.music.discordrpc.entities.Ready
import com.metrolist.music.discordrpc.entities.Resume
import io.ktor.client.HttpClient
import io.ktor.client.plugins.ResponseException
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import timber.log.Timber
import java.util.Locale
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import android.os.Build

// open: the module's unit tests subclass it with a recording/fake gateway (see
// DiscordRpcConnectionStaleGuardTest) without pulling in a mocking framework.
open class GatewayWebSocket(
    private val token: String,
    private val os: String,
    private val browser: String,
    private val device: String,
    private val gatewayUrl: String = GATEWAY_URL,
    private val clientFactory: () -> HttpClient = { HttpClient { install(WebSockets) } },
) : CoroutineScope {
    private val job = SupervisorJob()
    override val coroutineContext = job + DiscordRpc.backgroundDispatcher + gatewayExceptionHandler
    private val tag = "DiscordGateway"

    private val client = clientFactory()
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }

    private var session: DefaultClientWebSocketSession? = null
    private var sessionId: String? = null
    private var sequence = 0
    private var resumeUrl: String? = null
    private var heartbeatInterval = 0L
    private var heartbeatJob: Job? = null
    private var connected = false
    private var sessionEstablished = false
    private var reconnectionJob: Job? = null
    private var currentReconnectDelay = INITIAL_RECONNECT_DELAY
    private var intentionalClose = false
    private var lastHeartbeatAckReceivedAt = 0L
    private var heartbeatWatchdogJob: Job? = null
    private var lastPresence: Presence? = null

    private val _reconnectAbandoned = MutableStateFlow(false)

    /** True once the gateway gave up retrying (see [MAX_RECONNECT_ATTEMPTS]). */
    val reconnectAbandoned: StateFlow<Boolean> = _reconnectAbandoned

    /** Consecutive failed automatic reconnections; exposed for tests. */
    internal var reconnectAttempts = 0

    open fun isSessionEstablished(): Boolean = sessionEstablished

    /**
     * Manual connection request (app-level: user reconnect, next setActivity after a
     * disconnect). Resets the reconnection budget so a fresh attempt cycle starts.
     */
    fun connect() {
        reconnectAttempts = 0
        _reconnectAbandoned.update { false }
        connectInternal()
    }

    private fun connectInternal() {
        if (connected) {
            Timber.tag(tag).d("connect() called but already connected")
            return
        }
        if (!isActive) {
            Timber.tag(tag).w("connect() called but scope is not active — was close() called?")
            return
        }
        Timber.tag(tag).i("Connecting to Gateway...")
        intentionalClose = false
        reconnectionJob?.cancel()
        reconnectionJob = launch {
            try {
                establishConnection()
            } catch (e: Exception) {
                Timber.tag(tag).e(e, "establishConnection() threw unhandled exception")
                scheduleReconnection()
            }
        }
    }

    private suspend fun establishConnection() {
        val url = resumeUrl ?: gatewayUrl
        val systemLocale = Locale.getDefault().toString().replace('_', '-')
        Timber.tag(tag).d("establishConnection: url=$url locale=$systemLocale")

        session = try {
            client.webSocketSession(url) {
                header("User-Agent", USER_AGENT)
                header("Accept-Language", systemLocale)
            }
        } catch (e: ResponseException) {
            // The gateway rate-limits the upgrade with 429 + a Retry-After header (upstream
            // parity: DiscordGateway.onFailure reads the header, parseRetryAfter() the delay).
            val status = e.response.status.value
            val retryAfterSeconds = e.response.headers[HttpHeaders.RetryAfter]?.toLongOrNull()
            Timber.tag(tag).e(e, "WebSocket upgrade rejected: HTTP $status on $url, retryAfter=${retryAfterSeconds}s")
            connected = false
            scheduleReconnection(rateLimited = status == 429, retryAfterSeconds = retryAfterSeconds)
            return
        } catch (e: Exception) {
            Timber.tag(tag).e(e, "WebSocket connection failed to $url")
            connected = false
            throw e
        }

        connected = true
        currentReconnectDelay = INITIAL_RECONNECT_DELAY
        reconnectAttempts = 0
        _reconnectAbandoned.update { false }
        Timber.tag(tag).i("WebSocket connected to $url")

        try {
            session!!.incoming.receiveAsFlow().collect { frame ->
                when (frame) {
                    is Frame.Text -> {
                        val text = frame.readText()
                        try {
                            val payload = json.decodeFromString<Payload>(text)
                            handlePayload(payload)
                        } catch (e: Exception) {
                            Timber.tag(tag).w(e, "Failed to decode payload (${text.length} bytes)")
                        }
                    }
                    else -> {
                        Timber.tag(tag).v("Ignored frame type: ${frame::class.simpleName}")
                    }
                }
            }
        } catch (e: Exception) {
            Timber.tag(tag).e(e, "WebSocket receive flow ended with exception")
        }

        if (!intentionalClose) {
            Timber.tag(tag).d("WebSocket receive flow ended normally, handling disconnect")
            handleDisconnect()
        }
    }

    private suspend fun handlePayload(payload: Payload) {
        payload.s?.let { sequence = it }

        when (payload.op) {
            OpCode.DISPATCH -> {
                Timber.tag(tag).d("DISPATCH | seq=$sequence | event=${payload.t}")
                handleDispatch(payload)
            }
            OpCode.HEARTBEAT -> {
                Timber.tag(tag).d("<- HEARTBEAT (server-requested)")
                sendHeartbeat()
            }
            OpCode.RECONNECT -> {
                Timber.tag(tag).w("<- RECONNECT — server requested reconnect")
                handleReconnect()
            }
            OpCode.INVALID_SESSION -> {
                val canResume = payload.d?.let { json.decodeFromJsonElement<Boolean>(it) } ?: false
                Timber.tag(tag).w("<- INVALID_SESSION | canResume=$canResume")
                handleInvalidSession(payload)
            }
            OpCode.HELLO -> {
                Timber.tag(tag).d("<- HELLO — starting handshake")
                handleHello(payload)
            }
            OpCode.HEARTBEAT_ACK -> {
                lastHeartbeatAckReceivedAt = System.currentTimeMillis()
                Timber.tag(tag).v("<- HEARTBEAT_ACK")
            }
            else -> {
                Timber.tag(tag).d("<- op=${payload.op} (${payload.op?.name})")
            }
        }
    }

    private suspend fun handleHello(payload: Payload) {
        val hello = json.decodeFromJsonElement<HeartbeatResponse>(payload.d!!)
        heartbeatInterval = hello.heartbeatInterval
        Timber.tag(tag).i("HELLO: heartbeat_interval=${heartbeatInterval}ms")

        val jitter = (0..<heartbeatInterval).random()
        Timber.tag(tag).d("First heartbeat with jitter=${jitter}ms")
        delay(jitter)
        sendHeartbeat()
        startHeartbeatLoop()
        startHeartbeatWatchdog()

        if (sessionId != null && sequence > 0) {
            Timber.tag(tag).i("Resuming session: sessionId=$sessionId seq=$sequence")
            sendResume(sessionId!!)
        } else {
            Timber.tag(tag).i("Sending Identify (fresh session)")
            sendIdentify()
        }
    }

    private suspend fun handleDispatch(payload: Payload) {
        when (payload.t) {
            "READY" -> {
                val ready = json.decodeFromJsonElement<Ready>(payload.d!!)
                sessionId = ready.sessionId
                resumeUrl = ready.resumeGatewayUrl?.let { "$it/?v=9&encoding=json" }
                sessionEstablished = true
                Timber.tag(tag).i("READY received")
                resendLastPresence()
            }
            "RESUMED" -> {
                sessionEstablished = true
                Timber.tag(tag).i("RESUMED — session re-established")
                resendLastPresence()
            }
            else -> {
                Timber.tag(tag).d("Unhandled dispatch: ${payload.t}")
            }
        }
    }

    private suspend fun handleReconnect() {
        session?.close(CloseReason(4000, "Reconnect requested"))
    }

    private suspend fun handleInvalidSession(payload: Payload) {
        val canResume = payload.d?.let { json.decodeFromJsonElement<Boolean>(it) } ?: false
        val sid = sessionId
        delay(1500)
        if (canResume && sid != null) {
            Timber.tag(tag).i("INVALID_SESSION: can resume, sending Resume")
            sendResume(sid)
        } else {
            Timber.tag(tag).i("INVALID_SESSION: cannot resume, sending fresh Identify")
            sessionId = null
            sequence = 0
            resumeUrl = null
            sessionEstablished = false
            sendIdentify()
        }
    }

    private suspend fun handleDisconnect() {
        heartbeatJob?.cancel()
        heartbeatWatchdogJob?.cancel()
        connected = false
        sessionEstablished = false
        val reason = session?.closeReason?.await()
        val code = reason?.code?.toInt() ?: -1
        val message = reason?.message ?: "unknown"
        Timber.tag(tag).w("Disconnected: code=$code reason=$message")

        when {
            code == 1000 -> {
                // Clean remote close: the session is over on purpose — reset and stay offline
                // (upstream parity: handleClose code 1000 && remote). A manual connect() or the
                // next setActivity re-establishes the connection.
                Timber.tag(tag).i("Clean remote close (1000) — resetting session, no reconnect")
                sessionId = null
                sequence = 0
                resumeUrl = null
            }
            code == 4004 -> {
                Timber.tag(tag).e("Token invalid (4004) — will not reconnect")
            }
            !reconnectsOnClose(code) -> {
                // 4014 (invalid shard — upstream SurfaceFatal) and any other terminal code.
                Timber.tag(tag).e("Close code $code — fatal, will not reconnect")
            }
            code == 4006 || code == 4008 -> {
                Timber.tag(tag).i("Session invalidated ($code), clearing state and reconnecting")
                sessionId = null
                sequence = 0
                resumeUrl = null
                scheduleReconnection()
            }
            code == 4000 -> {
                Timber.tag(tag).d("Close code 4000 — immediate reconnect")
                if (beginReconnectionAttempt()) {
                    delay(200.milliseconds)
                    // Automatic reconnect: must NOT reset the budget (see connect()).
                    connectInternal()
                }
            }
            else -> {
                Timber.tag(tag).d("Close code $code — scheduling reconnection")
                scheduleReconnection()
            }
        }
    }

    private suspend fun sendIdentify() {
        val props = IdentifyProperties(
            os = os,
            browser = browser,
            device = device,
            systemLocale = Locale.getDefault().toString(),
            clientVersion = "314.13 - Stable",
            releaseChannel = "googleRelease",
            osVersion = Build.VERSION.RELEASE,
            osSdkVersion = Build.VERSION.SDK_INT.toString(),
            clientBuildNumber = 314013,
        )
        Timber.tag(tag).i("-> IDENTIFY: os=$os browser=$browser device=$device")
        send(
            op = OpCode.IDENTIFY,
            d = Identify(
                token = token,
                properties = props,
                presence = Presence(status = "online", since = null, afk = false),
                clientState = ClientState(),
            ),
        )
    }

    private suspend fun sendResume(sid: String) {
        Timber.tag(tag).i("-> RESUME: sessionId=$sid seq=$sequence")
        send(
            op = OpCode.RESUME,
            d = Resume(token = token, sessionId = sid, seq = sequence),
        )
    }

    private suspend fun sendHeartbeat() {
        val seq = if (sequence == 0) null else sequence
        Timber.tag(tag).v("-> HEARTBEAT seq=$seq")
        send(op = OpCode.HEARTBEAT, d = seq)
    }

    private fun startHeartbeatLoop() {
        heartbeatJob?.cancel()
        heartbeatJob = launch {
            Timber.tag(tag).d("Heartbeat loop started (interval=${heartbeatInterval}ms)")
            lastHeartbeatAckReceivedAt = System.currentTimeMillis()
            while (isActive) {
                delay(heartbeatInterval)
                sendHeartbeat()
            }
            Timber.tag(tag).d("Heartbeat loop ended")
        }
    }

    private fun startHeartbeatWatchdog() {
        heartbeatWatchdogJob?.cancel()
        heartbeatWatchdogJob = launch {
            val threshold = (heartbeatInterval * 2).coerceAtLeast(10_000L)
            while (isActive) {
                delay(threshold)
                val elapsed = System.currentTimeMillis() - lastHeartbeatAckReceivedAt
                if (elapsed >= threshold && connected) {
                    Timber.tag(tag).w("Heartbeat ACK not received for ${elapsed}ms — forcing reconnect")
                    handleReconnect()
                }
            }
        }
    }

    /**
     * Counts one automatic reconnection attempt against [MAX_RECONNECT_ATTEMPTS].
     * Returns false — and marks the gateway abandoned — once the budget is spent,
     * so a flapping network can never produce an unbounded reconnect loop.
     */
    private fun beginReconnectionAttempt(): Boolean {
        if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
            if (!_reconnectAbandoned.value) {
                Timber.tag(tag).e("Reconnection abandoned after $reconnectAttempts attempts — RPC stays offline until a manual connect()")
                _reconnectAbandoned.update { true }
            }
            return false
        }
        reconnectAttempts += 1
        return true
    }

    private fun scheduleReconnection(rateLimited: Boolean = false, retryAfterSeconds: Long? = null) {
        if (intentionalClose) {
            Timber.tag(tag).d("scheduleReconnection: intentionalClose=true, skipping")
            return
        }
        if (!beginReconnectionAttempt()) return
        val delay = reconnectDelayFor(rateLimited, retryAfterSeconds, currentReconnectDelay)
        Timber.tag(tag).d("scheduleReconnection: delay=${delay.inWholeSeconds}s attempt=$reconnectAttempts rateLimited=$rateLimited")
        reconnectionJob?.cancel()
        reconnectionJob = launch {
            delay(delay)
            currentReconnectDelay = (currentReconnectDelay * 2).coerceAtMost(MAX_RECONNECT_DELAY)
            Timber.tag(tag).d("Reconnection delay elapsed, calling connect()")
            // Automatic reconnect: must NOT reset the budget (see connect()).
            connectInternal()
        }
    }

    /**
     * Delay before the next reconnection attempt (upstream parity: DiscordGateway.reconnectDelayMs
     * + parseRetryAfter): a 429 rate-limited close waits the imposed Retry-After, never below 60 s;
     * any other failure uses the current exponential backoff with ±25 % jitter. Pure, exposed for
     * unit tests.
     */
    internal fun reconnectDelayFor(rateLimited: Boolean, retryAfterSeconds: Long?, currentDelay: Duration): Duration =
        when {
            rateLimited ->
                ((retryAfterSeconds ?: MIN_RATE_LIMIT_DELAY_SECONDS).coerceAtLeast(MIN_RATE_LIMIT_DELAY_SECONDS) * 1000L).milliseconds
            else -> applyJitter(currentDelay)
        }

    /**
     * ±[ratio] jitter on a delay (upstream parity: DiscordGateway.applyJitter with ratio 0.25 on
     * the reconnect backoff) — de-synchronizes reconnects so a fleet of devices does not retry on
     * the same tick.
     */
    internal fun applyJitter(base: Duration, ratio: Double = RECONNECT_JITTER): Duration {
        val ms = base.inWholeMilliseconds
        if (ms <= 0L) return base
        val delta = (ms * ratio).toLong()
        if (delta <= 0L) return base
        val offset = Random.nextLong(delta + 1)
        return (ms + if (Random.nextBoolean()) offset else -offset).coerceAtLeast(0).milliseconds
    }

    /**
     * Which close codes allow an automatic reconnection (upstream parity:
     * DiscordReconnectStrategy.decide, minus the OAuth refresh actions this fork never had).
     * 1000 (clean remote close), 4004 (invalid token), 4014 (invalid shard) are terminal.
     * Pure, exposed for unit tests; handleDisconnect() is built on the same table.
     */
    internal fun reconnectsOnClose(code: Int): Boolean = code !in setOf(1000, 4004, 4014)

    open suspend fun updatePresence(presence: Presence, staleCheck: (() -> Boolean)? = null) {
        Timber.tag(tag).d("updatePresence: waiting for sessionEstablished...")
        val startTime = System.currentTimeMillis()
        var waited = 0L
        while (!sessionEstablished) {
            delay(10.milliseconds)
            waited += 10
            if (waited > 30_000L) {
                Timber.tag(tag).w("updatePresence: timed out waiting for session (30s)")
                return
            }
        }
        // The wait above can take up to 30s: re-validate staleness right before
        // writing, not only at the call site, or a superseded presence could still
        // land after a newer activity/clear arrived during the wait.
        if (staleCheck != null && !staleCheck()) {
            Timber.tag(tag).w("updatePresence: superseded while waiting for session — skipping")
            return
        }
        Timber.tag(tag).d("updatePresence: session ready after ${System.currentTimeMillis() - startTime}ms")
        lastPresence = presence
        Timber.tag(tag).i("-> PRESENCE_UPDATE: activities=${presence.activities.size}")
        send(op = OpCode.PRESENCE_UPDATE, d = presence)
        Timber.tag(tag).d("updatePresence: sent in ${System.currentTimeMillis() - startTime}ms")
    }

    private suspend fun resendLastPresence() {
        val presence = lastPresence ?: return
        Timber.tag(tag).i("Re-sending last presence after session recovery")
        send(op = OpCode.PRESENCE_UPDATE, d = presence)
    }

    open suspend fun clearPresence() {
        if (sessionEstablished) {
            Timber.tag(tag).i("-> PRESENCE_UPDATE (clearing)")
            send(
                op = OpCode.PRESENCE_UPDATE,
                d = Presence(activities = emptyList(), since = null, status = "online", afk = false),
            )
        }
        // Forget the previous presence so a reconnection (READY/RESUMED dispatch)
        // never re-sends an activity the app no longer wants: resendLastPresence()
        // would otherwise resurrect a stale "Browsing" status after the user
        // disabled it. Clearing while the session is down must also drop the
        // memory, or the next READY dispatch re-sends it.
        lastPresence = null
        Timber.tag(tag).d("clearPresence: lastPresence cleared (no stale re-send on reconnect)")
    }

    private suspend inline fun <reified T> send(op: OpCode, d: T?) {
        if (session?.isActive == true) {
            val payload = json.encodeToString(
                Payload(
                    op = op,
                    d = if (d != null) json.encodeToJsonElement(d) else null,
                ),
            )
            session?.send(Frame.Text(payload))
        } else {
            Timber.tag(tag).w("Cannot send ${op.name}: session is not active")
        }
    }

    fun close() {
        Timber.tag(tag).i("close() called — stopping connection")
        intentionalClose = true
        reconnectionJob?.cancel()
        heartbeatJob?.cancel()
        heartbeatWatchdogJob?.cancel()
        launch {
            try {
                session?.close()
            } catch (_: Exception) { }
            // Release the client only after the session close handshake: its engine (OkHttp
            // dispatcher + connection pool) hosts the socket, and shutting it down first would
            // drop the close frame. Upstream parity: DiscordGateway.closeHttp().
            client.close()
        }
        connected = false
        sessionEstablished = false
    }

    companion object {
        private const val GATEWAY_URL = "wss://gateway.discord.gg/?v=9&encoding=json"
        private const val USER_AGENT = "Discord-Android/314013;RNA"

        internal val gatewayExceptionHandler = CoroutineExceptionHandler { _, e ->
            Timber.tag("DiscordGateway").e(e, "GatewayWebSocket scope coroutine failed")
        }

        private val INITIAL_RECONNECT_DELAY = 1.seconds
        private val MAX_RECONNECT_DELAY = 60.seconds

        /** Automatic reconnections allowed before the gateway gives up (upstream parity). */
        internal const val MAX_RECONNECT_ATTEMPTS = 7

        /** ±25 % jitter on the reconnect backoff (upstream parity: applyJitter(base, 0.25)). */
        internal const val RECONNECT_JITTER = 0.25

        /** A 429 without a usable Retry-After waits at least this long (upstream parity: 60 s floor). */
        private const val MIN_RATE_LIMIT_DELAY_SECONDS = 60L
    }
}
