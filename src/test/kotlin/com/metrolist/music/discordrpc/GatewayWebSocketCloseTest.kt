// The RecordingEngine below implements ktor's HttpClientEngine, which is @InternalAPI in Ktor 3
// (engine implementations are an internal extension point). The opt-in is test-only: production
// code never implements an engine, it only calls HttpClient.close().
@file:OptIn(InternalAPI::class)

package com.metrolist.music.discordrpc

import io.ktor.client.HttpClient
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.HttpClientEngineConfig
import io.ktor.client.plugins.HttpClientPlugin
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.util.AttributeKey
import io.ktor.utils.io.InternalAPI
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * close() used to run `runBlocking { session?.close() }` — a blocking call reachable from Main
 * via DiscordRpcConnection.closeDirect(). It now launches on the class's own CoroutineScope
 * ([DiscordRpc.backgroundDispatcher]) instead, so the caller is never blocked waiting for the
 * network close handshake.
 *
 * A full regression test proving the caller thread is never blocked would require mocking ktor's
 * DefaultClientWebSocketSession (a complex external type with no test seam) to simulate a slow
 * close — judged disproportionate for this hygiene fix. This test instead locks down the
 * observable, currently-testable contract: close() is safe to call repeatedly and never leaves
 * the instance's root Job cancelled (job lifecycle is intentionally untouched by this fix).
 */
class GatewayWebSocketCloseTest {

    @Test
    fun `close is safe to call before connect and does not cancel the root job`() {
        val gateway = GatewayWebSocket(token = "t", os = "Android", browser = "b", device = "d")

        gateway.close()
        gateway.close()

        val rootJob = requireNotNull(gateway.coroutineContext[Job]) { "GatewayWebSocket must expose a root Job" }
        assertFalse(rootJob.isCancelled, "close() must not cancel the scope's root Job (out of scope for this fix)")
    }

    /**
     * close() must release the injected http client (upstream parity: DiscordGateway.closeHttp) —
     * every connection cycle (token change, manager stop) creates a fresh client, so a client
     * that is never closed leaks its engine per cycle. The client is built on a recording engine
     * plus a recorder plugin: HttpClient.close() closes the installed AutoCloseable plugin
     * instances, so a closed recorder proves the gateway actually closed its client.
     */
    @Test
    fun `close releases the http client after the session handshake`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            val recorder = CloseRecorder()
            val gateway = GatewayWebSocket(
                token = "t",
                os = "Android",
                browser = "b",
                device = "d",
                clientFactory = {
                    HttpClient(RecordingEngine()) {
                        install(CloseRecorderPlugin(recorder))
                    }
                },
            )

            gateway.close()
            testScheduler.runCurrent()

            assertTrue(recorder.closed, "close() must close the http client")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /** Recorder closed by HttpClient.close() (installed AutoCloseable plugins are released). */
    private class CloseRecorder : AutoCloseable {
        var closed = false

        override fun close() {
            closed = true
        }
    }

    /** Installs a [CloseRecorder] as a plugin so HttpClient.close() closes it. */
    private class CloseRecorderPlugin(private val recorder: CloseRecorder) :
        HttpClientPlugin<Unit, CloseRecorder> {
        override val key: AttributeKey<CloseRecorder> = AttributeKey("closeRecorder")

        override fun prepare(block: (Unit) -> Unit): CloseRecorder {
            block(Unit)
            return recorder
        }

        override fun install(plugin: CloseRecorder, client: HttpClient) {
        }
    }

    /** Minimal engine that never executes requests. */
    private class RecordingEngine : HttpClientEngine {
        override val coroutineContext: kotlin.coroutines.CoroutineContext = Dispatchers.Unconfined
        override val dispatcher: CoroutineDispatcher = Dispatchers.Unconfined
        override val config: HttpClientEngineConfig = HttpClientEngineConfig()

        override fun close() {
        }

        override suspend fun execute(request: HttpRequestData): HttpResponseData =
            error("RecordingEngine does not execute requests")
    }
}
