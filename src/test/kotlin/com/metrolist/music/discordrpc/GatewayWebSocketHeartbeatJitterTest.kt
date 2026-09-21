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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * Heartbeat jitter tests (item 3): the jittered interval is ±5 % of the negotiated interval
 * (upstream formula) and is computed ONCE per session — the loop reuses the same value at
 * every tick instead of re-rolling the randomness (upstream startHeartbeat parity).
 */
class GatewayWebSocketHeartbeatJitterTest {

    private companion object {
        // 200 ms keeps the loop under the 10 s watchdog threshold while making the
        // ±5 % bound (190..210) observable.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":200}}"""
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

    @Test
    fun `applyHeartbeatJitter stays within plus-or-minus five percent of the interval`() {
        val gateway = GatewayWebSocket("t", "Android", "b", "d")
        try {
            repeat(200) {
                val jittered = gateway.applyHeartbeatJitter(45_000)
                assertTrue(
                    jittered in 42_750..47_250,
                    "jittered interval $jittered must stay within ±5% of 45000ms",
                )
            }
            // Small intervals: the ±5% delta floors at 0, so the interval is unchanged.
            assertEquals(0L, gateway.applyHeartbeatJitter(0))
            assertEquals(1L, gateway.applyHeartbeatJitter(1))
            assertEquals(10L, gateway.applyHeartbeatJitter(10))
        } finally {
            gateway.close()
        }
    }

    @Test
    fun `the heartbeat loop reuses one stable jittered interval for the whole session`() = runTest {
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

            // Collect the first four op:1 frames (the one-shot first heartbeat — sent
            // immediately, without the HELLO jitter — then the loop ticks) with their
            // virtual timestamps. A constant jittered interval means the loop ticks are
            // exactly equally spaced — re-rolling the jitter per tick would make the
            // spacing drift. The one-shot frame is drained one ms late (it was written
            // before the collection loop started), so the stability check starts at the
            // first two loop ticks.
            val ticks = mutableListOf<Long>()
            while (ticks.size < 4) {
                testScheduler.advanceTimeBy(1)
                testScheduler.runCurrent()
                while (true) {
                    val result = session.outgoing.tryReceive()
                    if (!result.isSuccess) break
                    val frame = result.getOrNull()
                    if (frame is Frame.Text && frame.readText().contains("\"op\":1")) {
                        ticks.add(testScheduler.currentTime)
                    }
                }
            }

            val delta1 = ticks[2] - ticks[1]
            val delta2 = ticks[3] - ticks[2]
            assertEquals(delta1, delta2, "the jittered interval must be constant for the whole session (computed once at loop start)")
            assertTrue(delta1 in 190..210, "the interval must stay within ±5% of 200ms (was $delta1)")

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
