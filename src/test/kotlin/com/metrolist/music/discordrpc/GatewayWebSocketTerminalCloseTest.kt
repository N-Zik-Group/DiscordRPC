// The ScriptedEngine below implements ktor's HttpClientEngine, which is @InternalAPI in Ktor 3
// (engine implementations are an internal extension point). The opt-in is test-only: production
// code never implements an engine.
@file:OptIn(InternalAPI::class)

package com.metrolist.music.discordrpc

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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * Terminal-close tests:
 * - 4004 (invalid token) surfaces as a durable terminal close code, resets the session state
 *   (no stale RESUME on the next connect), does not consume the reconnection budget and does
 *   not abandon (item 9);
 * - a 429 on the second attempt honors the Retry-After header on top of the 60 s floor
 *   (item 12 — the f1c2b44 design, verification test for the chained case).
 */
class GatewayWebSocketTerminalCloseTest {

    private companion object {
        // heartbeat_interval=1 keeps the HELLO jitter delay at 0 ((0..<1).random() == 0), so a
        // single runCurrent() completes the whole handshake; the watchdog threshold floors at
        // 10 s and checks real time, so it stays out of every test's window.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":1}}"""
        const val READY =
            """{"op":0,"t":"READY","s":1,"d":{"v":10,"session_id":"test-session","resume_gateway_url":"wss://resume.example/abc"}}"""
    }

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

        // A locally initiated close is answered the way a real session would be: the peer
        // echoes the close frame, completing closeReason and ending the incoming stream.
        override suspend fun send(frame: Frame) {
            outgoing.send(frame)
            if (frame is Frame.Close) {
                if (!closeReason.isCompleted) closeReason.complete(frame.toCloseReason())
                writeIncoming.close()
                incoming.close()
            }
        }

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

    private class ScriptedEngine(
        private val nextResponse: (HttpRequestData) -> HttpResponseData,
    ) : HttpClientEngine {
        var upgradeCalls = 0
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

    /** Consumes every frame the gateway has enqueued on [session] so far. */
    private suspend fun drain(session: FakeWebSocketSession): List<String> {
        val out = mutableListOf<String>()
        while (true) {
            val result = session.outgoing.tryReceive()
            if (!result.isSuccess) return out
            val frame = result.getOrNull()
            if (frame is Frame.Text) out.add(frame.readText())
        }
    }

    private val json = Json { ignoreUnknownKeys = true }

    private fun framesOfOp(frames: List<String>, op: Int): List<kotlinx.serialization.json.JsonObject> =
        frames.mapNotNull { text ->
            runCatching { json.parseToJsonElement(text).jsonObject }.getOrNull()
                ?.takeIf { it["op"]?.jsonPrimitive?.intOrNull == op }
        }

    @Test
    fun `a 4004 close sets the terminal code, resets state, and connect starts from a clean slate`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var session: FakeWebSocketSession
            lateinit var freshSession: FakeWebSocketSession
            lateinit var engine: ScriptedEngine
            engine = ScriptedEngine {
                if (engine.upgradeCalls == 1) {
                    session = FakeWebSocketSession()
                } else {
                    freshSession = FakeWebSocketSession()
                }
                upgradeResponse(if (engine.upgradeCalls == 1) session else freshSession)
            }
            val gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // The server rejects the token.
            session.feed(Frame.Close(CloseReason(4004, "Auth failed")))
            testScheduler.runCurrent()

            assertEquals(4004, gateway.terminalCloseCode.value, "the terminal close code must be surfaced")
            assertEquals(0, gateway.reconnectAttempts, "a terminal 4004 must not consume the budget")
            assertFalse(gateway.reconnectAbandoned.value, "a terminal 4004 is not an abandonment")

            testScheduler.advanceTimeBy(30_000)
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls, "a terminal 4004 must not schedule a reconnection")

            // A manual connect clears the terminal code and starts a fresh cycle.
            gateway.connect()
            testScheduler.runCurrent()
            assertNull(gateway.terminalCloseCode.value, "connect() must clear the terminal close code")
            assertEquals(2, engine.upgradeCalls, "connect() must open a fresh socket")

            // The 4004 branch reset the session state: the fresh socket identifies instead of
            // resuming the stale session.
            freshSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            val frames = drain(freshSession)
            assertTrue(framesOfOp(frames, 2).isNotEmpty(), "after a 4004 reset the fresh socket must identify")
            assertTrue(framesOfOp(frames, 6).isEmpty(), "after a 4004 reset the fresh socket must not resume the stale session")

            freshSession.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            freshSession.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a chained 429 honors the 60s floor first and the Retry-After second`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var gateway: GatewayWebSocket
            lateinit var session: FakeWebSocketSession
            lateinit var engine: ScriptedEngine
            engine = ScriptedEngine {
                when (engine.upgradeCalls) {
                    1 -> {
                        // Stand in for the production OkHttp interceptor (captures status + Retry-After).
                        gateway.lastUpgradeRefusal = GatewayWebSocket.UpgradeRefusal(status = 429, retryAfterSeconds = null)
                        rateLimitResponse(null)
                    }
                    2 -> {
                        gateway.lastUpgradeRefusal = GatewayWebSocket.UpgradeRefusal(status = 429, retryAfterSeconds = 90)
                        rateLimitResponse(90)
                    }
                    else -> {
                        session = FakeWebSocketSession()
                        upgradeResponse(session)
                    }
                }
            }
            gateway = newGateway(engine)
            gateway.connect()

            // Attempt 1: 429 without header -> 60 s floor.
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the first failed attempt must count against the budget")

            testScheduler.advanceTimeBy(59_000)
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "a header-less 429 must still wait the 60s floor")

            // Attempt 2: 429 with Retry-After: 90 -> waits the full 90 s on top.
            testScheduler.advanceTimeBy(1_000)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the second attempt must fire after the full 60s floor")
            assertEquals(2, gateway.reconnectAttempts, "the second failed attempt must count against the budget")

            testScheduler.advanceTimeBy(89_000)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the 90s Retry-After must not be cut short")

            testScheduler.advanceTimeBy(1_000)
            testScheduler.runCurrent()
            assertEquals(3, engine.upgradeCalls, "the third attempt must fire at t=150s (60s + 90s)")
            assertEquals(0, gateway.reconnectAttempts, "a successful connection must reset the counter")
            assertFalse(gateway.reconnectAbandoned.value)

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
