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
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpProtocolVersion
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
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * INVALID_SESSION (item 18) and op 7 RECONNECT verification tests:
 * - op 9 with d: false resets session state BEFORE closing, so the reconnection on the
 *   fresh socket IDENTIFIES (not resumes a dead session);
 * - op 9 with d: true keeps the state, so the fresh socket RESUMES with the old session id;
 * - both close the socket with 4000 (no resume/identify in situ on an invalidated socket)
 *   and the reconnect waits the standard 200 ms (the historical delay(1500) is gone);
 * - op 7 RECONNECT keeps its existing behavior: close 4000, resume on the fresh socket.
 */
class GatewayWebSocketInvalidSessionTest {

    private companion object {
        // heartbeat_interval=1 keeps the HELLO jitter delay at 0 ((0..<1).random() == 0), so a
        // single runCurrent() completes the whole handshake; the watchdog threshold floors at
        // 10 s and checks real time, so it stays out of every test's window.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":1}}"""
        const val READY =
            """{"op":0,"t":"READY","s":1,"d":{"v":10,"session_id":"test-session","resume_gateway_url":"wss://resume.example/abc"}}"""
        const val RESUMED = """{"op":0,"t":"RESUMED","s":1}"""
        const val INVALID_SESSION_NO_RESUME = """{"op":9,"d":false}"""
        const val INVALID_SESSION_RESUMABLE = """{"op":9,"d":true}"""
        const val RECONNECT = """{"op":7}"""
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
    fun `invalid session without resume resets state and re-identifies on the fresh socket`() = runTest {
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

            // The server invalidates the session, non-resumable.
            session.feed(Frame.Text(INVALID_SESSION_NO_RESUME))
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the 4000 reconnect must count against the budget")
            assertEquals(1, engine.upgradeCalls, "the reconnect must wait the standard 200 ms (no legacy long delay)")

            testScheduler.advanceTimeBy(199)
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls, "the reconnect must still be parked at 199 ms")

            testScheduler.advanceTimeBy(1)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the reconnect must open a fresh socket after the 200 ms")

            // The fresh socket identifies (the reset cleared sessionId), never resumes.
            freshSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            val frames = drain(freshSession)
            assertTrue(framesOfOp(frames, 2).isNotEmpty(), "a non-resumable invalidation must re-identify on the fresh socket")
            assertTrue(framesOfOp(frames, 6).isEmpty(), "a non-resumable invalidation must not resume the dead session")

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
    fun `invalid session with resume keeps state and resumes on the fresh socket`() = runTest {
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

            // The server invalidates the session, but the state is resumable.
            session.feed(Frame.Text(INVALID_SESSION_RESUMABLE))
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(200)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the reconnect must open a fresh socket after the 200 ms")

            // The fresh socket resumes with the old session id and the kept sequence.
            freshSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            val resume = framesOfOp(drain(freshSession), 6)
            assertEquals(1, resume.size, "a resumable invalidation must resume on the fresh socket")
            val d = requireNotNull(resume.first()["d"]?.jsonObject) { "the RESUME frame must carry a d object" }
            assertEquals("test-session", d["session_id"]?.jsonPrimitive?.content, "the resume must carry the old session id")
            assertEquals(1, d["seq"]?.jsonPrimitive?.intOrNull, "the resume must carry the kept sequence")
            assertEquals("t", d["token"]?.jsonPrimitive?.content)

            freshSession.feed(Frame.Text(RESUMED))
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
    fun `an op 7 reconnect request closes with 4000 and resumes on the fresh socket`() = runTest {
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

            // The server asks for a reconnect (existing fork behavior, unchanged).
            session.feed(Frame.Text(RECONNECT))
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the 4000 reconnect must count against the budget")
            testScheduler.advanceTimeBy(200)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the op 7 reconnect must open a fresh socket")

            // The state was kept, so the fresh socket resumes.
            freshSession.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            val resume = framesOfOp(drain(freshSession), 6)
            assertEquals(1, resume.size, "the op 7 reconnect must resume with the kept session state")
            val d = requireNotNull(resume.first()["d"]?.jsonObject) { "the RESUME frame must carry a d object" }
            assertEquals("test-session", d["session_id"]?.jsonPrimitive?.content)

            freshSession.feed(Frame.Text(RESUMED))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            freshSession.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
