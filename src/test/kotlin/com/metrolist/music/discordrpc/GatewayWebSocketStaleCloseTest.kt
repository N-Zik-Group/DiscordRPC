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
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * Generation-guard tests (item 1): a close event from a session that is no longer the active
 * one must not reset state, must not surface a terminal code, and must never spawn a ghost
 * reconnection against the live session.
 */
class GatewayWebSocketStaleCloseTest {

    private companion object {
        // heartbeat_interval=1 keeps the HELLO jitter delay at 0 ((0..<1).random() == 0), so a
        // single runCurrent() completes the whole handshake; the watchdog threshold floors at
        // 10 s and checks real time, so it stays out of every test's window.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":1}}"""
        const val READY =
            """{"op":0,"t":"READY","s":1,"d":{"v":10,"session_id":"test-session","resume_gateway_url":"wss://resume.example/abc"}}"""
    }

    /**
     * In-memory [DefaultWebSocketSession] (pass-through for Ktor's convertSessionToDefault)
     * backed by two channels. Like GatewayWebSocketUpgradeTest, a fed Close frame completes
     * [closeReason] with its exact reason and ends [incoming].
     *
     * Additionally: Ktor's close(reason) extension (and the gateway's own send helper) funnel
     * through the virtual [send] member, so a locally initiated close is answered the way a
     * real session would be — the peer echoes the close frame, which completes [closeReason]
     * and ends [incoming]. Without that answer a local close() would only enqueue a Close
     * frame on the unobserved outgoing channel and the gateway's receive loop (and hence
     * handleDisconnect) would hang.
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

        override suspend fun send(frame: Frame) {
            outgoing.send(frame)
            if (frame is Frame.Close) {
                if (!closeReason.isCompleted) closeReason.complete(frame.toCloseReason())
                writeIncoming.close()
                incoming.close()
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
     * Engine that answers upgrade requests from a script (same shape as the one in
     * GatewayWebSocketUpgradeTest).
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
    fun `a close event from a session invalidated by close is ignored`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var session: FakeWebSocketSession
            val engine = ScriptedEngine {
                session = FakeWebSocketSession()
                upgradeResponse(session)
            }
            val gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // Invalidate the active generation and release the client.
            gateway.close()
            testScheduler.runCurrent()

            // A 4004 close frame from the dead session lands after the invalidation: it must
            // not surface a terminal error and must not schedule anything.
            session.feed(Frame.Close(CloseReason(4004, "Auth failed")))
            testScheduler.runCurrent()

            assertNull(gateway.terminalCloseCode.value, "a stale close must not set the terminal close code")
            testScheduler.advanceTimeBy(30_000)
            testScheduler.runCurrent()
            assertEquals(1, engine.upgradeCalls, "a stale close must not schedule a reconnection")
            assertFalse(gateway.reconnectAbandoned.value)
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a manual connect during the 4000 parking window supersedes the parked reconnect without a ghost attempt`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var session: FakeWebSocketSession
            val engine = ScriptedEngine {
                session = FakeWebSocketSession()
                upgradeResponse(session)
            }
            val gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // A 4000 close parks the reconnection in its 200 ms delay.
            session.feed(Frame.Close(CloseReason(4000, "Reconnect requested")))
            testScheduler.runCurrent()
            assertEquals(1, gateway.reconnectAttempts, "the 4000 reconnect must count against the budget")

            // A manual connect() lands in the parking window: it supersedes the parked attempt.
            gateway.connect()
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the manual connect must open a fresh session")
            assertEquals(0, gateway.reconnectAttempts, "connect() resets the budget")

            // The superseded attempt must not schedule a ghost reconnection: its backoff job
            // would cancel the live session's job and cascade into false abandonments.
            testScheduler.advanceTimeBy(30_000)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "a superseded attempt must not schedule a ghost reconnection")
            assertEquals(0, gateway.reconnectAttempts, "a superseded attempt must not consume the budget")
            assertFalse(gateway.reconnectAbandoned.value, "a superseded attempt must not abandon the reconnection")

            // The live session is healthy: it can still establish.
            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished(), "the live session must stay usable after the supersession")

            // Tear the session down before the test ends: runTest drains the scheduler after
            // the body, and a live heartbeat loop (delay(1) with heartbeat_interval=1) would
            // spin virtual time forever, filling the unbounded outgoing channel until OOM.
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
