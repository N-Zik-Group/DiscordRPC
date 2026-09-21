package com.metrolist.music.discordrpc

import com.metrolist.music.discordrpc.entities.Presence
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Two-phase presence tests (item 4): setActivity sends the text-only presence IMMEDIATELY
 * and resolves the images in a cancellable job, re-sending with the resolved assets only if
 * the write still won the race. The upload is cancelled by any newer setActivity (even
 * no-image), by clearActivity and by close/closeDirect — and a cancelled upload never writes
 * to the cache.
 */
class DiscordRpcConnectionTwoPhaseTest {

    /**
     * Stand-in gateway: session always "established" so setActivity never triggers a real
     * connect, and updatePresence optionally parks on a per-call gate (one gate per
     * updatePresence call, in call order) to simulate the in-flight window.
     */
    private class RecordingGateway : GatewayWebSocket("t", "Android", "b", "d") {
        val presences = mutableListOf<Presence>()
        var clearCount = 0
        private var callIndex = 0
        val gates: MutableList<CompletableDeferred<Unit>?> = mutableListOf()

        override fun isSessionEstablished(): Boolean = true

        override suspend fun updatePresence(presence: Presence, staleCheck: (() -> Boolean)?) {
            gates.getOrNull(callIndex)?.await()
            if (staleCheck != null && !staleCheck()) return
            callIndex++
            presences.add(presence)
        }

        override suspend fun clearPresence() {
            clearCount++
        }
    }

    private fun connectionWith(
        fake: RecordingGateway,
        fetcher: (suspend (String) -> String?)? = null,
    ): DiscordRpcConnection =
        DiscordRpcConnection("t", gatewayFactory = { _, _, _, _ -> fake }, externalAssetFetcher = fetcher)

    private fun activityOf(index: Int, presences: List<Presence>) = presences[index].activities.first()

    @Test
    fun `setActivity without images sends exactly one text-only presence`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val connection = connectionWith(fake)

            connection.setActivity(name = "Song X")
            testScheduler.runCurrent()

            assertEquals(1, fake.presences.size, "a no-image setActivity must produce exactly one send")
            assertNull(activityOf(0, fake.presences).assets, "the single send must be text-only (no assets block)")
            assertEquals("Song X", activityOf(0, fake.presences).name)
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * An mp: image needs no network round-trip, but the two-phase contract still applies:
     * the text-only presence lands immediately and the (instant) resolution re-sends with
     * the asset — the mp: path shares the same code path as the upload path.
     */
    @Test
    fun `an mp image sends twice — text only first, then with the asset`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val connection = connectionWith(fake)

            connection.setActivity(name = "Song A", largeImage = "mp:art-123", largeText = "Cover")
            testScheduler.runCurrent()

            assertEquals(2, fake.presences.size, "the two-phase send must produce text-only then asset sends")
            assertNull(activityOf(0, fake.presences).assets, "the first send must be text-only")
            val assets = activityOf(1, fake.presences).assets
            assertNotNull(assets, "the second send must carry the asset")
            assertEquals("mp:art-123", assets?.largeImage, "an mp: image must be kept as-is (no upload)")
            assertEquals("Cover", assets?.largeText)
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `an http image resolves through the external asset fetcher and re-sends with the uploaded asset`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val connection = connectionWith(fake) { "mp:uploaded-1" }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/c.png", largeText = "Cover")
            testScheduler.runCurrent()

            assertEquals(2, fake.presences.size)
            assertNull(activityOf(0, fake.presences).assets, "the first send must be text-only")
            val assets = activityOf(1, fake.presences).assets
            assertNotNull(assets, "the second send must carry the resolved assets")
            assertEquals("mp:uploaded-1", assets?.largeImage, "the http image must be replaced by the uploaded mp: asset")
            assertEquals(1, ArtworkCache.size, "the resolution must be cached")
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a newer setActivity cancels the in-flight image resolution`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetchGate = CompletableDeferred<Unit>()
            // The upload never completes on its own: it must be cancelled by the supersede.
            val connection = connectionWith(fake) { fetchGate.await(); "mp:never" }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the text-only first send must land immediately")
            assertNull(activityOf(0, fake.presences).assets)

            // A newer activity supersedes the in-flight upload (even without images: the
            // cancel precedes the no-image early return, upstream RM L361 parity).
            connection.setActivity(name = "Song B")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms debounce (virtual time)
            testScheduler.runCurrent()

            assertEquals(2, fake.presences.size, "the superseding setActivity must send its own text-only presence")
            assertEquals("Song B", activityOf(1, fake.presences).name)
            assertFalse(
                fake.presences.any { it.activities.any { a -> a.assets != null } },
                "the superseded upload must never re-send a stale activity",
            )

            // Release the parked fetcher: the cancelled job must not write to the cache.
            fetchGate.complete(Unit)
            testScheduler.runCurrent()
            assertEquals(0, ArtworkCache.size, "a cancelled upload must not leave a cache entry")
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `closeDirect cancels the in-flight image resolution without a cache write`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetchGate = CompletableDeferred<Unit>()
            val connection = connectionWith(fake) { fetchGate.await(); "mp:never" }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the text-only first send must land immediately")

            // The connection goes away mid-upload (idle timeout, token change, …).
            connection.closeDirect()
            testScheduler.runCurrent()

            fetchGate.complete(Unit)
            testScheduler.runCurrent()

            assertEquals(1, fake.presences.size, "the cancelled upload must not re-send the activity")
            assertEquals(0, ArtworkCache.size, "a cancelled upload must not leave a cache entry")
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `clearActivity wins over an in-flight image resolution`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetchGate = CompletableDeferred<Unit>()
            val connection = connectionWith(fake) { fetchGate.await(); "mp:never" }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the text-only first send must land immediately")

            // The user clears the activity while the upload is in flight (browsing toggle).
            connection.clearActivity()
            testScheduler.runCurrent()

            fetchGate.complete(Unit)
            testScheduler.runCurrent()

            assertEquals(1, fake.presences.size, "the cleared activity must not be resurrected by its upload")
            assertEquals(1, fake.clearCount, "the clear must reach the gateway")
            assertEquals(0, ArtworkCache.size, "a cancelled upload must not leave a cache entry")
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    @Test
    fun `a superseded phase-2 send is dropped by the activity id re-check`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            // The upload resolves immediately; the gate parks the SECOND presence write
            // itself (inside updatePresence) so a newer activity can supersede mid-write.
            fake.gates += null
            val gate = CompletableDeferred<Unit>()
            fake.gates += gate
            val connection = connectionWith(fake) { "mp:up" }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the text-only first send must land immediately")

            // Start the newer activity in its own coroutine: its activity id is bumped at
            // entry (before the debounce delay parks it), so by the time the parked second
            // write resumes it is already superseded. (The test dispatcher resumes eagerly,
            // so the gate must be released only AFTER the id bump.)
            val newer = launch { connection.setActivity(name = "Song B") }
            testScheduler.runCurrent()

            // Release the parked second write: the activity id re-check must drop it.
            gate.complete(Unit)
            testScheduler.runCurrent()

            // The newer activity still sends its own presence after its debounce.
            testScheduler.advanceTimeBy(500)
            testScheduler.runCurrent()
            assertTrue(newer.isCompleted, "the newer activity must have sent its presence")

            assertEquals(2, fake.presences.size, "the superseded second send must be dropped by the activity id re-check")
            assertEquals("Song B", activityOf(1, fake.presences).name)
            assertFalse(
                fake.presences.any { it.activities.any { a -> a.assets != null } },
                "the dropped send must not carry the resolved asset",
            )
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * PW-7 regression: the app re-sends the SAME artwork every refresh tick (5 s) to move
     * the timestamps. A same-image supersede must NOT cancel the in-flight upload — an
     * unconditional cancel killed a slow first upload on every tick, so the presence stayed
     * image-less indefinitely while playback was stable. The activityId guard still drops
     * the first tick's stale second send; the finished upload lands in the cache and the
     * current tick re-sends phase 2 with the image.
     */
    @Test
    fun `a same-image supersede keeps the in-flight upload running and the image appears on the next tick`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetchGate = CompletableDeferred<Unit>()
            var cancelled = false
            val connection = connectionWith(fake) {
                try {
                    fetchGate.await()
                    "mp:ok"
                } catch (e: CancellationException) {
                    cancelled = true
                    throw e
                }
            }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t0")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the text-only first send must land immediately")

            // The app's refresh tick re-sends the SAME artwork a few seconds later:
            // only the timestamps move, the artwork is unchanged.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t5")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms limit (virtual time)
            testScheduler.runCurrent()
            assertFalse(
                cancelled,
                "a same-image supersede must NOT cancel the in-flight upload (PW-7)",
            )

            // The slow upload completes: the cache holds the asset and the current tick
            // re-sends phase 2 with the resolved image (the first tick's stale phase-2
            // send is dropped by the activityId re-check).
            fetchGate.complete(Unit)
            testScheduler.runCurrent()
            assertEquals(1, ArtworkCache.size, "the finished upload must land in the cache despite the supersede")
            assertTrue(
                fake.presences.any { it.activities.any { a -> a.assets?.largeImage == "mp:ok" } },
                "the presence must carry the uploaded image",
            )
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * PW-7 contrast: a supersede with a DIFFERENT image still cancels the in-flight upload
     * immediately (the old image must not re-send for a superseded activity).
     */
    @Test
    fun `a different-image supersede still cancels the in-flight upload`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetchGate = CompletableDeferred<Unit>()
            var cancelledUpload = false
            val connection = connectionWith(fake) {
                try {
                    fetchGate.await()
                    "mp:never"
                } catch (e: CancellationException) {
                    cancelledUpload = true
                    throw e
                }
            }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()

            // A newer activity with a DIFFERENT image supersedes the in-flight upload.
            connection.setActivity(name = "Song B", largeImage = "http://img.example/b.png")
            testScheduler.advanceTimeBy(500)
            testScheduler.runCurrent()

            fetchGate.complete(Unit)
            testScheduler.runCurrent()

            assertTrue(cancelledUpload, "a different-image supersede must cancel the in-flight upload")
            // The cancelled upload (image A) must not land in the cache; only the new
            // image B's fetch completes and caches.
            assertNull(
                ArtworkCache.getOrFetch("http://img.example/a.png") { null },
                "the cancelled upload must not leave a cache entry",
            )
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
