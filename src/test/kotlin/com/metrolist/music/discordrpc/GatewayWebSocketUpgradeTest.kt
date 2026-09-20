// The ScriptedEngine below implements ktor's HttpClientEngine, which is @InternalAPI in Ktor 3
// (engine implementations are an internal extension point). The opt-in is test-only: production
// code never implements an engine.
@file:OptIn(InternalAPI::class)

package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.Presence
import io.ktor.client.HttpClient
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.HttpClientEngineCapability
import io.ktor.client.engine.HttpClientEngineConfig
import io.ktor.client.plugins.websocket.WebSocketCapability
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpProtocolVersion
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.util.date.GMTDate
import io.ktor.utils.io.InternalAPI
import io.ktor.websocket.CloseReason
import io.ktor.websocket.DefaultWebSocketSession
import io.ktor.websocket.Frame
import io.ktor.websocket.WebSocketExtension
import io.ktor.websocket.WebSocketSession
import io.ktor.websocket.readText
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * End-to-end upgrade tests with a scripted fake engine — the branch wiring that the pure-helper
 * tests (GatewayWebSocketReconnectPolicyTest) do not reach:
 * - a 429 refusal waits the imposed rate-limit delay (Retry-After, 60 s floor) before the next
 *   attempt — the real Ktor pipeline turns the engine's 429 into its own exception, exactly
 *   like production, and the refusal is read from [GatewayWebSocket.lastUpgradeRefusal]
 *   (in production the default clientFactory's OkHttp interceptor captures it, with the
 *   Retry-After header that the exception itself does not carry);
 * - a successful connection resets the attempt counter;
 * - close codes route correctly: 4000 reconnects against the budget, 1000 stays offline,
 *   4004 is terminal without consuming the budget;
 * - updatePresence aborts early when the gateway is closed while waiting, and a presence
 *   superseded during the session wait is dropped by the production re-check.
 */
class GatewayWebSocketUpgradeTest {

    private companion object {
        // heartbeat_interval=1 keeps the HELLO jitter delay at 0 ((0..<1).random() == 0), so a
        // single runCurrent() completes the whole handshake; the watchdog threshold floors at
        // 10 s and checks real time, so it stays out of every test's window.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":1}}"""
        // READY arrives as a DISPATCH (op:0) in Discord; op:11 is HEARTBEAT_ACK and would
        // be routed to the ack branch, never establishing the session.
        const val READY =
            """{"op":0,"t":"READY","s":1,"d":{"v":9,"session_id":"test-session","resume_gateway_url":"wss://gateway.example/?v=9"}}"""
    }

    /**
     * In-memory [DefaultWebSocketSession] (pass-through for Ktor's convertSessionToDefault)
     * backed by two channels. [feed] writes server frames: a Close frame completes
     * [closeReason] with its exact reason and ends [incoming] (the channel the gateway
     * actually reads), mirroring how a real session terminates on a remote close.
     */
    private class FakeWebSocketSession : WebSocketSession, DefaultWebSocketSession {
        override val coroutineContext: CoroutineContext = Dispatchers.Unconfined
        override var masking: Boolean = false
        override var maxFrameSize: Long = 64 * 1024
        override var pingIntervalMillis: Long = 0L
        override var timeoutMillis: Long = 0L
        override val closeReason = CompletableDeferred<CloseReason>()
        override val incoming: Channel<Frame> = Channel(Channel.UNLIMITED)
        override val outgoing: Channel<Frame> = Channel(Channel.UNLIMITED)
        override val extensions: List<WebSocketExtension<out Any>> = emptyList()

        private val writeIncoming: Channel<Frame> = Channel(Channel.UNLIMITED)

        init {
            launch {
                for (frame in writeIncoming) {
                    if (frame is Frame.Close) {
                        if (!closeReason.isCompleted) closeReason.complete(frame.toCloseReason())
                        incoming.close()
                    } else {
                        incoming.trySend(frame)
                    }
                }
            }
        }

        // Ktor 3's Frame.Close carries the reason as payload: 2-byte code + UTF-8 message.
        private fun Frame.Close.toCloseReason(): CloseReason {
            val data = this.data
            val code = if (data.size >= 2) {
                (((data[0].toInt() and 0xFF) shl 8) or (data[1].toInt() and 0xFF)).toShort()
            } else {
                -1
            }
            val message = if (data.size > 2) data.copyOfRange(2, data.size).toString(Charsets.UTF_8) else ""
            return CloseReason(code, message)
        }

        /** Delivers a server frame to the gateway (Text or Close). */
        fun feed(frame: Frame) {
            writeIncoming.trySend(frame)
        }

        override suspend fun flush() {
        }

        override fun terminate() {
            writeIncoming.close()
            incoming.close()
            outgoing.close()
            if (!closeReason.isCompleted) closeReason.complete(CloseReason(1000, ""))
        }

        override fun start(extensions: List<WebSocketExtension<*>>) {
        }
    }

    /**
     * Engine that answers upgrade requests from a script. Ktor 3's WebSockets plugin reads the
     * refusal from the response (status != 101 with the request body still a WebSocketContent),
     * so a plain 429 [HttpResponseData] produces the same exception production sees.
     */
    private class ScriptedEngine(
        private val nextResponse: (HttpRequestData) -> HttpResponseData,
    ) : HttpClientEngine {
        var upgradeCalls = 0
        // Ktor derives the engine's closed state from the Job in its coroutine context:
        // without one the engine reports itself closed before the first request.
        private val job = SupervisorJob()
        override val coroutineContext: CoroutineContext = Dispatchers.Unconfined + job
        override val dispatcher: CoroutineDispatcher = Dispatchers.Unconfined
        override val config: HttpClientEngineConfig = HttpClientEngineConfig()
        override val supportedCapabilities: Set<HttpClientEngineCapability<*>> = setOf(WebSocketCapability)

        override fun close() {
            job.cancel()
        }

        override suspend fun execute(request: HttpRequestData): HttpResponseData {
            upgradeCalls++
            return nextResponse(request)
        }
    }

    // Ktor requires a Job in the response call context (it structures the response
    // processing); a bare EmptyCoroutineContext fails inside the pipeline.
    private val responseCallContext: CoroutineContext = Dispatchers.Unconfined + SupervisorJob()

    /** A 429 upgrade refusal, optionally with a Retry-After header. */
    private fun rateLimitResponse(retryAfterSeconds: Long?): HttpResponseData = HttpResponseData(
        statusCode = HttpStatusCode.TooManyRequests,
        requestTime = GMTDate(),
        headers = if (retryAfterSeconds != null) {
            headersOf(HttpHeaders.RetryAfter, retryAfterSeconds.toString())
        } else {
            Headers.Empty
        },
        version = HttpProtocolVersion.HTTP_1_1,
        body = ByteArray(0),
        callContext = responseCallContext,
    )

    /** A successful upgrade: Ktor 3 engines carry the session in the 101 response body. */
    private fun upgradeResponse(session: FakeWebSocketSession): HttpResponseData = HttpResponseData(
        statusCode = HttpStatusCode.SwitchingProtocols,
        requestTime = GMTDate(),
        headers = Headers.Empty,
        version = HttpProtocolVersion.HTTP_1_1,
        body = session,
        callContext = responseCallContext,
    )

    private fun TestScope.newGateway(engine: ScriptedEngine): GatewayWebSocket = GatewayWebSocket(
        token = "t",
        os = "Android",
        browser = "b",
        device = "d",
        clientFactory = { HttpClient(engine) { install(WebSockets) } },
    )

    @Test
    fun `a 429 upgrade refusal waits the imposed delay before the next attempt`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            val engine = ScriptedEngine {
                // Stand in for the production OkHttp interceptor (captures status + Retry-After).
                gateway.lastUpgradeRefusal = GatewayWebSocket.UpgradeRefusal(status = 429, retryAfterSeconds = 90)
                rateLimitResponse(90)
            }
            gateway = newGateway(engine)
            gateway.connect()

            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the first failed attempt must count against the budget")

            testScheduler.advanceTimeBy(89_000)
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the 90s Retry-After must not be cut short")

            testScheduler.advanceTimeBy(1_000)
            testScheduler.runCurrent()
            assertEquals(2, gateway.reconnectAttempts, "the second attempt must fire after the full 90s")
            assertFalse(gateway.reconnectAbandoned.value)
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a 429 without a usable Retry-After waits the 60s floor`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            val engine = ScriptedEngine {
                gateway.lastUpgradeRefusal = GatewayWebSocket.UpgradeRefusal(status = 429, retryAfterSeconds = null)
                rateLimitResponse(null)
            }
            gateway = newGateway(engine)
            gateway.connect()

            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts)

            testScheduler.advanceTimeBy(59_000)
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "a header-less 429 must still wait the 60s floor")

            testScheduler.advanceTimeBy(1_000)
            testScheduler.runCurrent()
            assertEquals(2, gateway.reconnectAttempts)
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a successful reconnection resets the attempt counter`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var session: FakeWebSocketSession
            lateinit var engine: ScriptedEngine
            engine = ScriptedEngine {
                if (engine.upgradeCalls == 1) {
                    gateway.lastUpgradeRefusal = GatewayWebSocket.UpgradeRefusal(status = 429, retryAfterSeconds = null)
                    rateLimitResponse(null)
                } else {
                    session = FakeWebSocketSession()
                    upgradeResponse(session)
                }
            }
            gateway = newGateway(engine)
            gateway.connect()

            // Attempt 1: 429 without header -> 60 s floor.
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts)

            // Attempt 2: 101 + session -> success, counter reset, budget fully restored.
            testScheduler.advanceTimeBy(60_000)
            testScheduler.runCurrent()
            assertEquals(0, gateway.reconnectAttempts, "a successful connection must reset the counter")
            assertFalse(gateway.reconnectAbandoned.value)
            assertEquals(2, engine.upgradeCalls)

            // Drive the handshake to READY: the session is really established end to end.
            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // A presence write now lands on the established session.
            val sent = mutableListOf<String>()
            val sentJob = launch {
                for (frame in session.outgoing) {
                    if (frame is Frame.Text) sent += frame.readText()
                }
            }
            gateway.updatePresence(Presence(activities = emptyList(), since = null, status = "online", afk = false))
            testScheduler.runCurrent()
            sentJob.cancel()
            assertTrue(sent.any { it.contains("\"op\":3") }, "a presence must be written once the session is established")

            // Tear the session down before the test ends: runTest drains the scheduler after the
            // body, and a live heartbeat loop (delay(1) with heartbeat_interval=1) would spin
            // virtual time forever, filling the unbounded outgoing channel until OOM.
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a 4000 close reconnects against the budget and a clean 1000 close stays offline`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var currentSession: FakeWebSocketSession
            val engine = ScriptedEngine {
                currentSession = FakeWebSocketSession()
                upgradeResponse(currentSession)
            }
            gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()
            assertEquals(0, gateway.reconnectAttempts)

            // Establish the session.
            currentSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            currentSession.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // A 4000 (server-requested reconnect) consumes one attempt and reconnects.
            currentSession.feed(Frame.Close(CloseReason(4000, "Reconnect requested")))
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the immediate 4000 reconnect must count against the budget")

            testScheduler.advanceTimeBy(200)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the 4000 reconnect must open a fresh session")

            // Re-establish the new session, then a clean remote close (1000): offline, no reconnect.
            currentSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            currentSession.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            currentSession.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
            assertFalse(gateway.isSessionEstablished())
            // The successful 4000 reconnect already reset the budget to zero; the clean close adds nothing.
            assertEquals(0, gateway.reconnectAttempts, "a clean close must not consume another attempt")

            testScheduler.advanceTimeBy(30_000)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "a clean close must not schedule a reconnection")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a 4004 close is terminal without consuming the budget or abandoning`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var currentSession: FakeWebSocketSession
            val engine = ScriptedEngine {
                currentSession = FakeWebSocketSession()
                upgradeResponse(currentSession)
            }
            gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            currentSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            currentSession.feed(Frame.Text(READY))
            testScheduler.runCurrent()

            currentSession.feed(Frame.Close(CloseReason(4004, "Auth failed")))
            testScheduler.runCurrent()

            assertEquals(0, gateway.reconnectAttempts, "a terminal 4004 must not consume the budget")
            assertFalse(gateway.reconnectAbandoned.value, "a terminal 4004 is not an abandonment")
            testScheduler.advanceTimeBy(30_000)
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls, "a terminal 4004 must not schedule a reconnection")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `updatePresence aborts early when the gateway is closed while waiting`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var currentSession: FakeWebSocketSession
            val engine = ScriptedEngine {
                currentSession = FakeWebSocketSession()
                upgradeResponse(currentSession)
            }
            gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()
            // No READY dispatch: sessionEstablished stays false, updatePresence must wait.

            val job = launch {
                gateway.updatePresence(Presence(activities = emptyList(), since = null, status = "online", afk = false))
            }
            testScheduler.runCurrent()
            assertFalse(job.isCompleted, "updatePresence must wait for the session to be established")

            gateway.close()
            testScheduler.advanceTimeBy(100)
            testScheduler.runCurrent()
            assertTrue(job.isCompleted, "close() must abort the session wait early (no full 30 s)")
            assertEquals(0, gateway.reconnectAttempts, "close() must not schedule anything against the budget")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `connect after close is ignored and consumes no budget`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            val engine = ScriptedEngine {
                upgradeResponse(FakeWebSocketSession())
            }
            gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls)

            gateway.close()
            testScheduler.runCurrent()

            // The lazy reconnect from setActivity() on a closed connection must be a no-op:
            // the client is released, so attempting would burn the budget on a dead client.
            gateway.connect()
            testScheduler.advanceTimeBy(60_000)
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls, "connect() after close() must not attempt a connection")
            assertEquals(0, gateway.reconnectAttempts, "the budget must not be consumed on a dead client")
            assertFalse(gateway.reconnectAbandoned.value, "no false abandonment on a dead client")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `updatePresence superseded while waiting for the session is dropped by the production re-check`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var currentSession: FakeWebSocketSession
            val engine = ScriptedEngine {
                currentSession = FakeWebSocketSession()
                upgradeResponse(currentSession)
            }
            gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            val sent = mutableListOf<String>()
            val sentJob = launch {
                for (frame in currentSession.outgoing) {
                    if (frame is Frame.Text) sent += frame.readText()
                }
            }
            var current = true
            val job = launch {
                gateway.updatePresence(
                    Presence(activities = emptyList(), since = null, status = "online", afk = false),
                    staleCheck = { current },
                )
            }
            testScheduler.runCurrent()
            assertFalse(job.isCompleted, "updatePresence must wait for the session to be established")

            // A newer write supersedes this one while it is still waiting.
            current = false

            // Now establish the session: the wait ends and the production re-check must drop the write.
            currentSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            currentSession.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            // Wake the 10 ms polling step of updatePresence so the post-wait re-check runs.
            testScheduler.advanceTimeBy(20)
            testScheduler.runCurrent()

            assertTrue(job.isCompleted, "updatePresence must return after the post-wait re-check")
            assertTrue(sent.none { it.contains("\"op\":3") }, "a superseded presence must not be written")

            // Tear the session down before the test ends: runTest drains the scheduler after the
            // body, and a live heartbeat loop (delay(1) with heartbeat_interval=1) would spin
            // virtual time forever, filling the unbounded outgoing channel until OOM.
            currentSession.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
            sentJob.cancel()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
