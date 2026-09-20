package com.metrolist.music.discordrpc

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The reconnection policy ported from upstream Metrolist (DiscordGateway / DiscordReconnectStrategy):
 * 429 rate-limit delays (Retry-After, 60 s floor), ±25 % backoff jitter, and the close codes that
 * must NOT trigger an automatic reconnect (1000 clean remote close, 4004 invalid token, 4014 invalid
 * shard). The helpers under test are pure functions, so no dispatcher or network is involved.
 */
class GatewayWebSocketReconnectPolicyTest {

    private val gateway = GatewayWebSocket(token = "t", os = "Android", browser = "b", device = "d")

    @Test
    fun `a 429 honors the Retry-After header with a 60s floor`() {
        assertEquals(90.seconds, gateway.reconnectDelayFor(rateLimited = true, retryAfterSeconds = 90, currentDelay = 2.seconds))
        assertEquals(
            60.seconds,
            gateway.reconnectDelayFor(rateLimited = true, retryAfterSeconds = 10, currentDelay = 2.seconds),
            "a below-floor Retry-After must be raised to 60s",
        )
        assertEquals(
            60.seconds,
            gateway.reconnectDelayFor(rateLimited = true, retryAfterSeconds = null, currentDelay = 2.seconds),
            "a 429 without a usable header must still wait the 60s floor",
        )
    }

    @Test
    fun `a non rate-limited failure keeps the current backoff delay within the ±25 % jitter band`() {
        repeat(50) {
            val delay = gateway.reconnectDelayFor(rateLimited = false, retryAfterSeconds = null, currentDelay = 4.seconds)
            assertTrue(delay in 3.seconds..5.seconds, "delay $delay outside ±25 % of the 4s backoff")
        }
    }

    @Test
    fun `applyJitter stays within the ±25 % band`() {
        repeat(100) {
            val jittered = gateway.applyJitter(2.seconds)
            assertTrue(jittered in 1_500.milliseconds..2_500.milliseconds, "jittered $jittered outside ±25 % of 2s")
        }
    }

    @Test
    fun `applyJitter leaves a zero delay untouched`() {
        assertEquals(0.milliseconds, gateway.applyJitter(0.milliseconds))
    }

    @Test
    fun `terminal close codes never trigger a reconnect`() {
        assertFalse(gateway.reconnectsOnClose(1000), "clean remote close must not reconnect")
        assertFalse(gateway.reconnectsOnClose(4004), "invalid token stays terminal")
        assertFalse(gateway.reconnectsOnClose(4014), "invalid shard is fatal (upstream SurfaceFatal)")
    }

    @Test
    fun `transient close codes keep reconnecting`() {
        assertTrue(gateway.reconnectsOnClose(4000))
        assertTrue(gateway.reconnectsOnClose(4001))
        assertTrue(gateway.reconnectsOnClose(4006))
        assertTrue(gateway.reconnectsOnClose(4008))
        assertTrue(gateway.reconnectsOnClose(-1), "a network drop without a close frame must reconnect")
    }
}
