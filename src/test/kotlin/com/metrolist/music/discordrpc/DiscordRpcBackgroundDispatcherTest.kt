package com.metrolist.music.discordrpc

import kotlin.coroutines.ContinuationInterceptor
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

/**
 * Locks down the [DiscordRpc.backgroundDispatcher] injection seam (issue #606, Goal G5):
 * the module defaults to Dispatchers.IO so it works standalone, and every scope derived
 * from the seam picks the dispatcher up at construction time — which is how the host app
 * wires NzikDispatchers.DATA once at startup (MainApplication.onCreate).
 */
class DiscordRpcBackgroundDispatcherTest {

    // Captured at instance creation: each JUnit test gets a fresh instance, and every test
    // that overrides the seam restores this value in @AfterEach, so this is the module default.
    private val originalDispatcher = DiscordRpc.backgroundDispatcher

    @AfterEach
    fun restoreSeam() {
        DiscordRpc.backgroundDispatcher = originalDispatcher
    }

    @Test
    fun `seam defaults to Dispatchers IO before the host app wires it`() {
        assertSame(Dispatchers.IO, originalDispatcher,
            "standalone default must be Dispatchers.IO so the module works without a host")
    }

    @Test
    fun `GatewayWebSocket picks up the seam dispatcher in its coroutine context`() {
        val testDispatcher = UnconfinedTestDispatcher()
        DiscordRpc.backgroundDispatcher = testDispatcher

        val gateway = GatewayWebSocket(token = "t", os = "Android", browser = "b", device = "d")
        try {
            assertSame(testDispatcher, gateway.coroutineContext[ContinuationInterceptor],
                "GatewayWebSocket must run on the seam dispatcher set before construction")
        } finally {
            // The constructor allocates a Ktor HttpClient; release it (session is null here,
            // so close() only tears down the scope and jobs).
            gateway.close()
        }
    }
}
