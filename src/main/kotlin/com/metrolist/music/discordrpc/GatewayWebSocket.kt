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
import io.ktor.client.engine.okhttp.OkHttpConfig
import io.ktor.client.engine.okhttp.OkHttpEngine
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
import java.util.Collections
import java.util.Locale
import java.util.WeakHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.abs
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
    private val clientFactory: () -> HttpClient = {
        // The default engine (OkHttp) sees the raw upgrade response before Ktor's pipeline
        // turns a refusal into a bare exception that carries no status or headers: record
        // the refusal so establishConnection() can honor a 429 and its Retry-After
        // (upstream parity: DiscordGateway.onFailure reads both off the OkHttp response).
        // A default-argument expression cannot reference instance state, so the capture
        // lives on a holder registered in the companion, keyed by the engine config.
        val holder = RefusalHolder()
        val config = OkHttpConfig().apply {
            addInterceptor { chain ->
                val response = chain.proceed(chain.request())
                if (response.code != 101) {
                    holder.value = UpgradeRefusal(
                        status = response.code,
                        retryAfterSeconds = response.header(HttpHeaders.RetryAfter)?.toLongOrNull(),
                    )
                }
                response
            }
        }
        registerRefusalHolder(config, holder)
        HttpClient(OkHttpEngine(config)) {
            install(WebSockets)
        }
    },
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
    private var closed = false
    private var lastHeartbeatAckReceivedAt = 0L
    private var heartbeatWatchdogJob: Job? = null
    private var lastPresence: Presence? = null

    private val _reconnectAbandoned = MutableStateFlow(false)

    /** True once the gateway gave up retrying (see [MAX_RECONNECT_ATTEMPTS]). */
    val reconnectAbandoned: StateFlow<Boolean> = _reconnectAbandoned

    private val _terminalCloseCode = MutableStateFlow<Int?>(null)

    /**
     * Terminal close code that ended the session (currently only 4004 — invalid token), or
     * null when none is pending. Cleared by a manual [connect] so a fresh attempt starts
     * from a clean slate (item 9: the app surfaces this as a durable error banner).
     */
    val terminalCloseCode: StateFlow<Int?> = _terminalCloseCode

    /**
     * Generation counter of the connection attempts (upstream parity:
     * DiscordGateway.activeWebSocketId). Each attempt claims a generation in
     * [connectInternal] before opening the socket; the receive loop, [handleDisconnect]
     * and the attempt's failure handler only act while that generation is still the
     * active one, so a close event from a session that has already been superseded
     * (reconnection) or invalidated ([close]) is ignored instead of resetting the live
     * session's state or scheduling a ghost reconnection.
     */
    private val sessionGeneration = AtomicInteger(0)

    /**
     * Consecutive failed automatic reconnections. Atomic: [connect] is called from the
     * app's threads while the reconnection loop runs on [DiscordRpc.backgroundDispatcher],
     * so the check-then-increment in [beginReconnectionAttempt] must not race.
     */
    private val reconnectAttemptsCounter = AtomicInteger(0)

    /** Consecutive failed automatic reconnections; exposed for tests. */
    internal val reconnectAttempts: Int get() = reconnectAttemptsCounter.get()

    /**
     * The HTTP refusal of the most recent upgrade attempt. In production the OkHttp
     * interceptor of the default [clientFactory] captures it (a refused upgrade surfaces
     * in Ktor as a bare exception with no status and no headers); tests set it directly.
     */
    @Volatile
    internal var lastUpgradeRefusal: UpgradeRefusal? = null

    /** An HTTP refusal of the WebSocket upgrade: status + Retry-After seconds when the header is present. */
    internal data class UpgradeRefusal(val status: Int, val retryAfterSeconds: Long?)

    /**
     * Pulls the production interceptor's capture into [lastUpgradeRefusal] and clears the
     * capture slot. No-op when the engine registered no holder (test fakes, custom engines).
     */
    internal fun consumeCapturedUpgradeRefusal() {
        val holder = lookupRefusalHolder(client.engine.config) ?: return
        val captured = holder.value ?: return
        holder.value = null
        lastUpgradeRefusal = captured
    }

    open fun isSessionEstablished(): Boolean = sessionEstablished

    /**
     * Manual connection request (app-level: user reconnect, next setActivity after a
     * disconnect). Resets the reconnection budget so a fresh attempt cycle starts.
     */
    fun connect() {
        reconnectAttemptsCounter.set(0)
        _reconnectAbandoned.update { false }
        _terminalCloseCode.update { null }
        connectInternal()
    }

    private fun connectInternal() {
        if (closed) {
            Timber.tag(tag).w("connect() called after close() — client already released, ignoring")
            return
        }
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
        // Claim this attempt's generation (upstream parity: a fresh activeWebSocketId per
        // connection) before anything else, so a session opened after close() or after a
        // competing connection is born stale — and a superseded attempt can detect it.
        val generation = sessionGeneration.incrementAndGet()
        // A superseded live session's heartbeat/watchdog must not keep firing against the
        // replacement socket before its own HELLO: its handleDisconnect returns early on
        // the stale-generation guard and would never cancel them (they are siblings of the
        // scope, not children of the superseded attempt).
        heartbeatJob?.cancel()
        heartbeatWatchdogJob?.cancel()
        reconnectionJob?.cancel()
        reconnectionJob = launch {
            try {
                establishConnection(generation)
            } catch (e: Exception) {
                // A superseded attempt (cancelled by a newer connect()/close() while
                // parked or in flight) must NOT schedule a reconnection: its backoff job
                // would cancel the live session's job and cascade into false abandonments
                // (upstream parity: only the active generation reconnects).
                if (generation == sessionGeneration.get()) {
                    Timber.tag(tag).e(e, "establishConnection() threw unhandled exception")
                    scheduleReconnection()
                } else {
                    Timber.tag(tag).w("Superseded attempt (generation=$generation, active=${sessionGeneration.get()}) — not scheduling a reconnection")
                }
            }
        }
    }

    private suspend fun establishConnection(generation: Int) {
        val url = resumeUrl ?: gatewayUrl
        val systemLocale = Locale.getDefault().toString().replace('_', '-')
        Timber.tag(tag).d("establishConnection: url=$url locale=$systemLocale generation=$generation")

        val ws = try {
            client.webSocketSession(url) {
                header("User-Agent", USER_AGENT)
                header("Accept-Language", systemLocale)
            }
        } catch (e: Exception) {
            // Ktor turns a refused upgrade into a bare exception (WebSocketException for
            // a non-101 status, or a cast failure for the missing session content) that
            // carries no status and no headers: the real HTTP refusal lives in
            // lastUpgradeRefusal, captured by the interceptor of the default clientFactory
            // (upstream parity: DiscordGateway.onFailure reads status + Retry-After from
            // the OkHttp response).
            consumeCapturedUpgradeRefusal()
            val refusal = lastUpgradeRefusal
            lastUpgradeRefusal = null
            Timber.tag(tag).e(e, "WebSocket connection to $url failed" + (if (refusal != null) " (HTTP ${refusal.status})" else ""))
            connected = false
            if (refusal?.status == 429) {
                scheduleReconnection(rateLimited = true, retryAfterSeconds = refusal.retryAfterSeconds)
                return
            }
            throw e
        }

        // Upstream parity (DiscordGateway.onOpen L172): only a session whose generation is
        // still the active one becomes THE session; a superseded one is closed and ignored.
        if (generation != sessionGeneration.get()) {
            Timber.tag(tag).w("Session opened but already superseded (generation=$generation, active=${sessionGeneration.get()}) — dropping it")
            runCatching { ws.close(CloseReason(1000, "Superseded")) }
            return
        }

        session = ws
        connected = true
        currentReconnectDelay = INITIAL_RECONNECT_DELAY
        reconnectAttemptsCounter.set(0)
        _reconnectAbandoned.update { false }
        Timber.tag(tag).i("WebSocket connected to $url")

        try {
            ws.incoming.receiveAsFlow().collect { frame ->
                // Stale-frame guard: a frame delivered from a session that has already been
                // superseded by a newer connection must not be processed (upstream parity:
                // onMessage checks wsId == activeWebSocketId).
                if (generation != sessionGeneration.get()) {
                    Timber.tag(tag).w("Ignoring frame from stale session (generation=$generation, active=${sessionGeneration.get()})")
                    return@collect
                }
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
            handleDisconnect(generation, ws)
        }
    }

    private suspend fun handlePayload(payload: Payload) {
        // Sequence guard (upstream parity: handleFrame L241-244): only a non-zero sequence
        // advances the counter — an `s: 0` payload must never reset it, or RESUME would
        // replay with seq 0 and the heartbeat would send `d: null` mid-session.
        val s = payload.s
        if (s != null && s > 0) {
            sequence = s
        }

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
                // canResume is decoded once, in handleInvalidSession (single-decode cleanup).
                Timber.tag(tag).w("<- INVALID_SESSION")
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

        if (sessionId != null && sequence > 0) {
            // Resume: keep the upstream order (jittered first heartbeat, then RESUME).
            val jitter = (0..<heartbeatInterval).random()
            Timber.tag(tag).d("First heartbeat with jitter=${jitter}ms")
            delay(jitter)
            sendHeartbeat()
            startHeartbeatLoop()
            startHeartbeatWatchdog()
            Timber.tag(tag).i("Resuming session: sessionId=$sessionId seq=$sequence")
            sendResume(sessionId!!)
        } else {
            // Fresh session: no jitter on the first heartbeat. The gateway only completes
            // the identify (READY) once it has received the first heartbeat, so parking
            // it behind the full HELLO jitter (up to heartbeat_interval, ~42 s) delayed
            // READY — and with it the first presence update — by the same amount. The
            // server answers the early heartbeat with a HEARTBEAT_ACK as usual; the loop
            // below keeps the jittered interval.
            Timber.tag(tag).i("Sending Identify (fresh session)")
            sendIdentify()
            Timber.tag(tag).d("First heartbeat sent immediately (no jitter)")
            sendHeartbeat()
            startHeartbeatLoop()
            startHeartbeatWatchdog()
        }
    }

    private suspend fun handleDispatch(payload: Payload) {
        when (payload.t) {
            "READY" -> {
                val ready = json.decodeFromJsonElement<Ready>(payload.d!!)
                sessionId = ready.sessionId
                // Upstream parity: the resume URL is used as-is (no query rewrite) — the
                // gateway's v=10 URL already carries the encoding params.
                resumeUrl = ready.resumeGatewayUrl
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
        Timber.tag(tag).w("<- INVALID_SESSION | canResume=$canResume")
        if (!canResume) {
            // Reset before the close so the reconnection on the fresh socket identifies
            // instead of resuming a dead session (upstream parity: L284-289).
            Timber.tag(tag).i("INVALID_SESSION: cannot resume — resetting session state")
            sessionId = null
            sequence = 0
            resumeUrl = null
            sessionEstablished = false
        } else {
            Timber.tag(tag).i("INVALID_SESSION: resumable — state kept for the resume on the fresh socket")
        }
        // Always close with 4000 (upstream parity: L291, in both cases) — no resume/identify
        // in situ on an invalidated socket. The reconnect flows through the existing 4000
        // path on a NEW socket: resume if session+seq>0, identify otherwise.
        session?.close(CloseReason(4000, "Invalid session"))
    }

    private suspend fun handleDisconnect(generation: Int, ws: DefaultClientWebSocketSession) {
        // Stale-close guard (upstream parity: handleClose L306-313): a close event from a
        // session that is no longer the active one (superseded by a reconnection, or the
        // gateway was closed) must not reset the live session's state or trigger anything.
        if (generation != sessionGeneration.get()) {
            Timber.tag(tag).w("Ignoring stale disconnect (generation=$generation, active=${sessionGeneration.get()})")
            return
        }
        heartbeatJob?.cancel()
        heartbeatWatchdogJob?.cancel()
        connected = false
        sessionEstablished = false
        // Read the close reason from the session that actually closed, not from `session`
        // (which may already point at the replacement session of a newer connection).
        val reason = ws.closeReason.await()
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
                // Surface the terminal error (item 9) and reset the session state: without
                // the reset, a later connect() would reuse the stale resumeUrl and send an
                // obsolete RESUME (L294-300 equivalent) for a dead token.
                _terminalCloseCode.update { 4004 }
                sessionId = null
                sequence = 0
                resumeUrl = null
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
        // Computed once per session: the jittered interval is stable for the whole loop
        // (upstream parity: startHeartbeat L406/L412 — a single applyJitter result reused
        // at every tick), which de-synchronizes the fleet's heartbeats without drifting
        // the per-session cadence.
        val jitteredInterval = applyHeartbeatJitter(heartbeatInterval)
        heartbeatJob = launch {
            Timber.tag(tag).d("Heartbeat loop started (interval=${heartbeatInterval}ms, jittered=${jitteredInterval}ms)")
            lastHeartbeatAckReceivedAt = System.currentTimeMillis()
            while (isActive) {
                delay(jitteredInterval)
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
        if (reconnectAttemptsCounter.get() >= MAX_RECONNECT_ATTEMPTS) {
            if (!_reconnectAbandoned.value) {
                Timber.tag(tag).e("Reconnection abandoned after $reconnectAttempts attempts — RPC stays offline until a manual connect()")
                _reconnectAbandoned.update { true }
            }
            return false
        }
        reconnectAttemptsCounter.incrementAndGet()
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
     * ±[ratio] jitter on the heartbeat interval (upstream parity: DiscordGateway.applyJitter
     * L481-488 with JITTER_RATIO 0.05, L406). Computed once per session by
     * [startHeartbeatLoop] and reused at every tick. Pure, exposed for unit tests.
     */
    internal fun applyHeartbeatJitter(interval: Long, ratio: Double = HEARTBEAT_JITTER): Long {
        if (interval <= 0L) return interval
        val delta = (interval * ratio).toLong()
        if (delta <= 0L) return interval
        val offset = abs(Random.nextLong(delta + 1))
        val sign = if (Random.nextBoolean()) -1L else 1L
        return interval + sign * offset
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
            // close() released the client, or the gateway abandoned its retry budget:
            // the session can never come up — do not sit out the full 30 s for nothing.
            if (closed || _reconnectAbandoned.value) {
                Timber.tag(tag).w("updatePresence: gateway closed or abandoned while waiting — aborting early")
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
        // Upstream parity (buildPresenceUpdate since: Long = 0): the presence update always
        // encodes `since` as 0 — Discord manages the "online since" itself; a null would
        // serialize as `"since": null` (explicitNulls is on by default) and reset the timer.
        val presenceToSend = presence.copy(since = presence.since ?: 0L)
        lastPresence = presenceToSend
        Timber.tag(tag).i("-> PRESENCE_UPDATE: activities=${presenceToSend.activities.size}")
        send(op = OpCode.PRESENCE_UPDATE, d = presenceToSend)
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
            // since = 0 parity: the clearing op 3 carries the same defaults as a normal
            // presence update (upstream: clear() → buildPresenceUpdate since: 0).
            send(
                op = OpCode.PRESENCE_UPDATE,
                d = Presence(activities = emptyList(), since = 0L, status = "online", afk = false),
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
        // Terminal: the client is released below, so a later connect()/setActivity() on
        // this instance must not start attempts against a dead client (they would burn
        // the reconnection budget and flip reconnectAbandoned — a false toast).
        closed = true
        // Invalidate the active generation: the dying session's close event must be
        // ignored by handleDisconnect instead of resetting state or scheduling anything.
        sessionGeneration.incrementAndGet()
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
        private const val GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
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

        /** ±5 % jitter on the heartbeat interval, computed once per session (upstream parity: JITTER_RATIO 0.05). */
        internal const val HEARTBEAT_JITTER = 0.05

        /** A 429 without a usable Retry-After waits at least this long (upstream parity: 60 s floor). */
        private const val MIN_RATE_LIMIT_DELAY_SECONDS = 60L

        /**
         * Refusal-capture holders for the default [clientFactory], keyed by the owning
         * client's engine config. A constructor default-argument expression cannot write
         * instance state, so the interceptor registers its holder here and
         * [consumeCapturedUpgradeRefusal] looks it up through the engine config.
         */
        private val refusalHolders = Collections.synchronizedMap(WeakHashMap<Any, RefusalHolder>())

        private fun registerRefusalHolder(config: Any, holder: RefusalHolder) {
            refusalHolders[config] = holder
        }

        private fun lookupRefusalHolder(config: Any): RefusalHolder? =
            refusalHolders[config]
    }
}

/**
 * Single mutable slot for [GatewayWebSocket.UpgradeRefusal], written by the OkHttp
 * interceptor of the default [GatewayWebSocket.clientFactory] and read by
 * [GatewayWebSocket.consumeCapturedUpgradeRefusal].
 */
internal class RefusalHolder {
    @Volatile
    var value: GatewayWebSocket.UpgradeRefusal? = null
}
