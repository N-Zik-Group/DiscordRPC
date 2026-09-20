package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.Presence
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * A setActivity call suspends while resolving images (and the gateway may wait up to 30s
 * for the session). During that window a newer setActivity or clearActivity may arrive
 * (song skip, browsing clear): the superseded write must be dropped so a stale activity
 * never lands after the newer one. The guard is the activityId counter, checked at the
 * connection layer and re-checked by the gateway right before the write.
 */
class DiscordRpcConnectionStaleGuardTest {

    /**
     * Stand-in gateway: session always "established" so setActivity never triggers a real
     * connect, and updatePresence parks on [gate] to simulate the in-flight window
     * (image resolution / session wait). It mirrors the production staleCheck behavior.
     */
    private class RecordingGateway : GatewayWebSocket("t", "Android", "b", "d") {
        val presences = mutableListOf<Presence>()
        var clearCount = 0
        var gate: CompletableDeferred<Unit>? = null

        override fun isSessionEstablished(): Boolean = true

        override suspend fun updatePresence(presence: Presence, staleCheck: (() -> Boolean)?) {
            gate?.await()
            if (staleCheck != null && !staleCheck()) return
            presences.add(presence)
        }

        override suspend fun clearPresence() {
            clearCount++
        }
    }

    private fun connectionWith(fake: RecordingGateway): DiscordRpcConnection =
        DiscordRpcConnection("t", gatewayFactory = { _, _, _, _ -> fake })

    @Test
    fun `clearActivity supersedes an in-flight setActivity`() = runTest {
        val fake = RecordingGateway()
        val connection = connectionWith(fake)
        val gate = CompletableDeferred<Unit>()
        fake.gate = gate

        launch {
            connection.setActivity(name = "Song A")
        }
        testScheduler.runCurrent() // A reaches the gate, mid-flight

        connection.clearActivity()
        gate.complete(Unit)
        testScheduler.runCurrent()

        assertTrue(fake.presences.isEmpty(), "the superseded setActivity must not reach the gateway")
        assertEquals(1, fake.clearCount)
        connection.closeDirect()
    }

    @Test
    fun `a newer setActivity supersedes an in-flight one`() = runTest {
        val fake = RecordingGateway()
        val connection = connectionWith(fake)
        val gate = CompletableDeferred<Unit>()
        fake.gate = gate

        launch {
            connection.setActivity(name = "Song A")
        }
        testScheduler.runCurrent() // A reaches the gate, mid-flight

        launch {
            connection.setActivity(name = "Song B")
        }
        // B is throttled by the 500ms min update interval — advance virtual time.
        testScheduler.advanceTimeBy(500)
        testScheduler.runCurrent() // B reaches the gate after bumping the activity id

        gate.complete(Unit)
        testScheduler.runCurrent()

        assertEquals(1, fake.presences.size, "only the most recent setActivity may be written")
        assertEquals("Song B", fake.presences.first().activities.first().name)
        connection.closeDirect()
    }

    @Test
    fun `setActivity writes a single presence on an established session`() = runTest {
        val fake = RecordingGateway()
        val connection = connectionWith(fake)

        connection.setActivity(name = "Song C")

        assertEquals(1, fake.presences.size)
        assertEquals("Song C", fake.presences.first().activities.first().name)
        connection.closeDirect()
    }
}
