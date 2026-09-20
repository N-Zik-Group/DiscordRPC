package com.metrolist.music.discordrpc

import io.ktor.client.HttpClient
import io.ktor.client.plugins.api.createClientPlugin
import io.ktor.client.plugins.websocket.WebSockets
import java.io.IOException
import java.util.Collections
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import timber.log.Timber

/**
 * The gateway must give up after [GatewayWebSocket.MAX_RECONNECT_ATTEMPTS] automatic
 * reconnections instead of retrying forever on a dead network, and a manual
 * [GatewayWebSocket.connect] (the lazy reconnect from DiscordRpcConnection.setActivity)
 * must restart a fresh cycle.
 *
 * Failure is injected with a ktor client plugin that throws in the request pipeline
 * (before any engine I/O), so every connection attempt fails synchronously on the test
 * thread and the whole backoff (1s, 2s, 4s, ...) runs on the test's virtual clock —
 * no real network, no cross-thread races, no flakiness.
 */
class GatewayWebSocketMaxAttemptsTest {

    private val capturedLogs = Collections.synchronizedList(mutableListOf<String>())

    private val captureTree = object : Timber.Tree() {
        override fun log(priority: Int, tag: String?, message: String, t: Throwable?) {
            capturedLogs.add("$tag: $message")
        }
    }

    private val failingClient = createClientPlugin("FailingClient") {
        onRequest { _, _ -> throw IOException("simulated network failure") }
    }

    @AfterEach
    fun tearDown() {
        if (Timber.forest().contains(captureTree)) {
            Timber.uproot(captureTree)
        }
        capturedLogs.clear()
    }

    private fun newFailingGateway(): GatewayWebSocket = GatewayWebSocket(
        token = "t",
        os = "Android",
        browser = "b",
        device = "d",
        // Unreachable loopback URL as a backstop: even if the plugin ever stops
        // intercepting the websocket upgrade request, the attempt still fails fast
        // instead of dialing the real Discord gateway from a unit test.
        gatewayUrl = "wss://127.0.0.1:1/?v=9&encoding=json",
        clientFactory = { HttpClient { install(WebSockets); install(failingClient) } },
    )

    private fun TestScope.driveUntilAbandoned(gateway: GatewayWebSocket) {
        // Backoff sequence is 1s, 2s, 4s, 8s, 16s, 32s, 60s — 65s per pass covers it.
        // Bounded: a stuck failure path must fail the test loudly, not spin forever.
        var passes = 0
        while (!gateway.reconnectAbandoned.value && passes < 50) {
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(65_000)
            passes++
            // Safety net: if a failure is still delivered from a real dispatcher thread
            // (engine backstop path), yield wall-clock time so it can reach the scheduler.
            Thread.sleep(20)
        }
        assertTrue(
            gateway.reconnectAbandoned.value,
            "gateway did not abandon after $passes virtual backoff passes (attempts=${gateway.reconnectAttempts}). " +
                "Gateway logs:\n" + capturedLogs.takeLast(15).joinToString("\n") { "  $it" },
        )
    }

    @Test
    fun `gateway abandons after max reconnect attempts and stops retrying`() = runTest {
        Timber.plant(captureTree)
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            val gateway = newFailingGateway()
            gateway.connect()

            driveUntilAbandoned(gateway)

            assertEquals(GatewayWebSocket.MAX_RECONNECT_ATTEMPTS, gateway.reconnectAttempts)

            // No 8th attempt once the budget is spent.
            testScheduler.runCurrent()
            testScheduler.advanceTimeBy(2 * 60_000)
            testScheduler.runCurrent()
            assertEquals(GatewayWebSocket.MAX_RECONNECT_ATTEMPTS, gateway.reconnectAttempts)
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `manual connect resets the attempt counter after abandonment`() = runTest {
        Timber.plant(captureTree)
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            val gateway = newFailingGateway()
            gateway.connect()
            driveUntilAbandoned(gateway)
            assertTrue(gateway.reconnectAbandoned.value)

            // A manual reconnect (e.g. the next setActivity) must start a fresh cycle:
            // the counter restarts from zero instead of being already abandoned.
            gateway.connect()
            assertFalse(gateway.reconnectAbandoned.value)

            // The first failure is delivered from a real dispatcher thread — wait (bounded)
            // for the first automatic retry to be scheduled. No virtual time is advanced
            // here: advancing it would fire the just-scheduled retry delay prematurely.
            var waited = 0
            while (gateway.reconnectAttempts == 0 && waited < 100) {
                Thread.sleep(20)
                testScheduler.runCurrent()
                waited++
            }
            assertEquals(1, gateway.reconnectAttempts)

            // And the full budget is available again.
            driveUntilAbandoned(gateway)
            assertTrue(gateway.reconnectAbandoned.value)
            assertEquals(GatewayWebSocket.MAX_RECONNECT_ATTEMPTS, gateway.reconnectAttempts)
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
