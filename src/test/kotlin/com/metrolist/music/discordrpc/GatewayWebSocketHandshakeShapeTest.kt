// The ScriptedEngine below implements ktor's HttpClientEngine, which is @InternalAPI in Ktor 3
// (engine implementations are an internal extension point). The opt-in is test-only: production
// code never implements an engine.
@file:OptIn(InternalAPI::class)

package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.Activity
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
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * Handshake shape tests: the gateway URL (item 2 — v10 initial, resume URL used as-is), the
 * sequence guard (item 14 — s=0 never resets the counter), the IDENTIFY payload audit
 * (item 19 — raw token, capabilities, no intents) and the op 3 presence shape (item 6 —
 * status/afk carried, since encoded as 0, activity type code).
 */
class GatewayWebSocketHandshakeShapeTest {

    private companion object {
        // heartbeat_interval=1 keeps the HELLO jitter delay at 0 ((0..<1).random() == 0) and
        // the jittered loop interval at exactly 1 ms, so ticks land on integer virtual times.
        const val HELLO = """{"op":10,"d":{"heartbeat_interval":1}}"""
        const val READY =
            """{"op":0,"t":"READY","s":1,"d":{"v":10,"session_id":"test-session","resume_gateway_url":"wss://resume.example/abc"}}"""
        const val GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encoding=json"
        const val RESUME_URL = "wss://resume.example/abc"
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

    /** Engine that answers upgrade requests from a script, recording the requested URLs. */
    private class ScriptedEngine(
        private val nextResponse: (HttpRequestData) -> HttpResponseData,
    ) : HttpClientEngine {
        var upgradeCalls = 0
        val upgradeUrls = mutableListOf<String>()
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
            upgradeUrls.add(request.url.toString())
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

    private fun TestScope.newGateway(
        engine: ScriptedEngine,
        token: String = "t",
    ): GatewayWebSocket = GatewayWebSocket(
        token = token,
        os = "Android",
        browser = "b",
        device = "d",
        clientFactory = { HttpClient(engine) { install(WebSockets) } },
    )

    /** Consumes every frame the gateway has enqueued so far (non-suspending per frame). */
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
    fun `the first upgrade uses the default gateway url and a reconnection reuses the resume url as-is`() = runTest {
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
            assertEquals(listOf(GATEWAY_URL), engine.upgradeUrls, "the initial upgrade must use the v10 gateway url")

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()
            session.feed(Frame.Text(READY))
            testScheduler.runCurrent()
            assertTrue(gateway.isSessionEstablished())

            // A 4000 close reconnects on the resume URL from READY — as-is, no rewrite.
            session.feed(Frame.Close(CloseReason(4000, "Reconnect requested")))
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(200)
            testScheduler.runCurrent()
            assertEquals(2, engine.upgradeCalls, "the 4000 reconnect must open a fresh session")
            assertEquals(RESUME_URL, engine.upgradeUrls[1], "the resume url must be used as-is (no rewrite)")

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

    @Test
    fun `the heartbeat sequence is only advanced by non-zero payloads`() = runTest {
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
            // Before any sequence-carrying payload the heartbeat carries d: null — the
            // JsonElement d field is always written, null included.
            val before = framesOfOp(drain(session), 1)
            assertEquals(1, before.size, "exactly the first (one-shot) heartbeat must have been sent")
            assertTrue(before.first()["d"] is JsonNull, "the first heartbeat must carry d: null while sequence == 0")

            // A dispatch with s: 5 advances the sequence: the next heartbeat carries d: 5.
            session.feed(Frame.Text("""{"op":0,"t":"SEQ","s":5}"""))
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(1)
            testScheduler.runCurrent()
            val afterFive = framesOfOp(drain(session), 1)
            assertEquals(1, afterFive.size)
            assertEquals(5, afterFive.first()["d"]?.jsonPrimitive?.intOrNull, "the heartbeat must carry the advanced sequence")

            // An s: 0 payload must NEVER reset the counter (RESUME would replay seq 0 and
            // the heartbeat would revert to d: null mid-session).
            session.feed(Frame.Text("""{"op":0,"t":"SEQ0","s":0}"""))
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(1)
            testScheduler.runCurrent()
            val afterZero = framesOfOp(drain(session), 1)
            assertEquals(1, afterZero.size)
            assertEquals(5, afterZero.first()["d"]?.jsonPrimitive?.intOrNull, "an s: 0 payload must not reset the sequence")

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a fresh session sends the first heartbeat immediately without the hello jitter`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var session: FakeWebSocketSession
            // A real-world interval: with the old jittered first heartbeat the delay would
            // have been up to 45 s, so an op:1 frame at virtual time 0 can only come from
            // the immediate first heartbeat.
            val helloLong = """{"op":10,"d":{"heartbeat_interval":45000}}"""
            val engine = ScriptedEngine {
                session = FakeWebSocketSession()
                upgradeResponse(session)
            }
            val gateway = newGateway(engine)
            gateway.connect()
            testScheduler.runCurrent()

            session.feed(Frame.Text(helloLong))
            testScheduler.runCurrent()

            val frames = drain(session)
            assertEquals(1, framesOfOp(frames, 2).size, "the fresh session must identify")
            assertEquals(1, framesOfOp(frames, 1).size, "the first heartbeat must be sent immediately (no HELLO jitter)")

            // The loop keeps the jittered interval (~45 s): nothing further within 1 s.
            testScheduler.advanceTimeBy(1_000)
            testScheduler.runCurrent()
            assertEquals(0, framesOfOp(drain(session), 1).size, "no loop tick within 1 s of a 45 s interval")

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `the identify payload keeps the fork shape (raw token, capabilities, no intents)`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            lateinit var session: FakeWebSocketSession
            val engine = ScriptedEngine {
                session = FakeWebSocketSession()
                upgradeResponse(session)
            }
            val gateway = newGateway(engine, token = "raw-token")
            gateway.connect()
            testScheduler.runCurrent()

            session.feed(Frame.Text(HELLO))
            testScheduler.runCurrent()

            val identify = framesOfOp(drain(session), 2)
            assertEquals(1, identify.size, "a fresh session must send exactly one IDENTIFY after HELLO")
            val d = requireNotNull(identify.first()["d"]?.jsonObject) { "the IDENTIFY frame must carry a d object" }

            // Item 19 audit: the fork's production shape — raw token (no "Bearer" prefix,
            // unlike upstream which prefixes at the call site), capabilities 16381,
            // properties + presence + client_state, and NO intents field.
            assertEquals("raw-token", d["token"]?.jsonPrimitive?.content, "the token must be sent raw, not Bearer-prefixed")
            assertEquals(16381, d["capabilities"]?.jsonPrimitive?.intOrNull, "capabilities must stay 16381")
            assertEquals("Android", d["properties"]?.jsonObject?.get("os")?.jsonPrimitive?.content)
            assertEquals("b", d["properties"]?.jsonObject?.get("browser")?.jsonPrimitive?.content)
            assertEquals("d", d["properties"]?.jsonObject?.get("device")?.jsonPrimitive?.content)
            assertTrue(d.containsKey("presence"), "the presence block must be present in the fork's IDENTIFY")
            assertEquals("online", d["presence"]?.jsonObject?.get("status")?.jsonPrimitive?.content)
            assertTrue(d.containsKey("client_state"), "the client_state block must be present")
            assertFalse(d.containsKey("intents"), "the fork sends no intents field")

            // The identify was accepted end to end: the session establishes.
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

    @Test
    fun `presence updates encode since as 0 and clearPresence carries the same defaults`() = runTest {
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

            // An op 3 presence update carries status + afk + the activity type code, and
            // encodes since as 0 (never null: explicitNulls would reset Discord's timer).
            gateway.updatePresence(
                Presence(
                    activities = listOf(Activity(name = "Song X", type = ActivityType.PLAYING.value)),
                    since = null,
                    status = "dnd",
                    afk = true,
                ),
            )
            testScheduler.runCurrent()
            val update = framesOfOp(drain(session), 3).last()
            val updateD = requireNotNull(update["d"]?.jsonObject) { "the presence frame must carry a d object" }
            assertEquals(0, updateD["since"]?.jsonPrimitive?.longOrNull, "since must be encoded 0, not null")
            assertEquals("dnd", updateD["status"]?.jsonPrimitive?.content, "the presence status must be carried")
            assertEquals(true, updateD["afk"]?.jsonPrimitive?.booleanOrNull, "the afk flag must be carried")
            val activities = updateD["activities"]?.jsonArray.orEmpty()
            assertEquals(1, activities.size, "the update must carry exactly one activity")
            val activity = activities.first().jsonObject
            assertEquals(0, activity["type"]?.jsonPrimitive?.intOrNull, "the activity type code must be carried")
            assertEquals("Song X", activity["name"]?.jsonPrimitive?.content)

            // clearPresence() sends the same op 3 defaults with an empty activity list.
            gateway.clearPresence()
            testScheduler.runCurrent()
            val clear = framesOfOp(drain(session), 3).last()
            val clearD = requireNotNull(clear["d"]?.jsonObject) { "the clearing frame must carry a d object" }
            assertEquals(0, clearD["since"]?.jsonPrimitive?.longOrNull, "the clearing update must encode since as 0")
            assertEquals("online", clearD["status"]?.jsonPrimitive?.content)
            assertEquals(false, clearD["afk"]?.jsonPrimitive?.booleanOrNull)
            assertTrue(clearD["activities"]?.jsonArray.orEmpty().isEmpty(), "the clearing update must carry no activities")

            // Tear the session down before the test ends (live heartbeat loop OOM hazard).
            session.feed(Frame.Close(CloseReason(1000, "Bye")))
            testScheduler.runCurrent()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
