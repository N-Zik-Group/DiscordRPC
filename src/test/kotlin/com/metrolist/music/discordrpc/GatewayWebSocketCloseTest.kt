package com.metrolist.music.discordrpc

import kotlinx.coroutines.Job
import org.junit.jupiter.api.Assertions.assertFalse
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
}
