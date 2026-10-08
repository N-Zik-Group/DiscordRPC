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
 * the write still won the race. The upload is cancelled by a newer setActivity with
 * DIFFERENT (or no) images, by clearActivity and by close/closeDirect — a newer setActivity
 * with the SAME images (the app's refresh tick) keeps the in-flight upload running — and a
 * cancelled upload never writes to the cache. Once an image has resolved, same-image ticks
 * reuse the remembered asset and send the complete presence in a single send.
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

    /**
     * Failed-upload regression: a failed upload must NOT fall back to the raw URL (Discord
     * cannot render it) and must NOT be cached (a cached raw URL would poison the
     * resolution for this artwork forever — the image stays dead until a token change).
     * The phase-1 text-only presence stays; the next update (the app's 5 s tick) retries
     * the upload on a cache miss and the presence then carries the image.
     */
    @Test
    fun `a failed upload is not cached and the next update retries it`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            var attempts = 0
            // The first upload fails (429 / network hiccup), the retry succeeds.
            val connection = connectionWith(fake) {
                attempts++
                if (attempts == 1) null else "mp:ok"
            }

            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size, "the failed upload must leave only the phase-1 text-only presence")
            assertNull(activityOf(0, fake.presences).assets, "no raw URL may be sent as an asset on failure")
            assertEquals(0, ArtworkCache.size, "a failed upload must NOT be cached (no raw-URL poisoning)")

            // The next update (the app's 5 s refresh tick on the same artwork) must RETRY
            // the upload — a cache miss.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t5")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms limit (virtual time)
            testScheduler.runCurrent()
            assertEquals(2, attempts, "the failed upload must be retried on the next update")
            assertEquals(1, ArtworkCache.size, "the retry's successful upload must be cached")
            assertTrue(
                fake.presences.any { it.activities.any { a -> a.assets?.largeImage == "mp:ok" } },
                "the presence must carry the image once the upload succeeds",
            )
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
     * Regression (same-artwork flash): the app re-sends the SAME artwork every refresh
     * tick (5 s) to move the timestamps. After the first two-phase send, every same-image
     * tick must send exactly ONE presence — already carrying the previously uploaded
     * asset — with no re-upload and no image-less frame (the old strip/re-add exchange
     * flickered the artwork on every tick and left the presence image-less whenever the
     * phase-2 send was slow, superseded or failed).
     */
    @Test
    fun `a repeated same-image setActivity reuses the uploaded asset and sends once`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            var fetchCalls = 0
            val connection = connectionWith(fake) {
                fetchCalls++
                "mp:ok"
            }

            // The first tick takes the normal two-phase path (text-only, then with asset).
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t0")
            testScheduler.runCurrent()
            assertEquals(2, fake.presences.size)
            assertNull(activityOf(0, fake.presences).assets)
            assertEquals("mp:ok", activityOf(1, fake.presences).assets?.largeImage)

            // The app's refresh tick re-sends the SAME artwork — only the details move.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t5")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()

            assertEquals(3, fake.presences.size, "a same-image tick must send exactly one presence")
            val tickAssets = activityOf(2, fake.presences).assets
            assertNotNull(tickAssets, "the tick presence must carry the artwork — no image drop, no flash")
            assertEquals("mp:ok", tickAssets?.largeImage, "the tick must reuse the previously uploaded asset")
            assertEquals(1, fetchCalls, "an unchanged artwork must never be re-uploaded")
            assertEquals(1, ArtworkCache.size)
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * Regression contrast (fallback): a failed upload is not remembered, so the very next
     * same-image tick still takes the two-phase path and retries the upload — once it
     * succeeds, subsequent ticks reuse the asset and send once.
     */
    @Test
    fun `after a failed upload the next same-image tick retries and later ticks reuse the asset`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            var attempts = 0
            val connection = connectionWith(fake) {
                attempts++
                if (attempts == 1) null else "mp:ok"
            }

            // First upload fails: only the text-only presence, nothing remembered.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(1, fake.presences.size)
            assertNull(activityOf(0, fake.presences).assets)

            // The next tick retries: two-phase again (the failed upload was not cached).
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t5")
            testScheduler.advanceTimeBy(500)
            testScheduler.runCurrent()
            assertEquals(2, attempts)
            assertTrue(
                fake.presences.any { it.activities.any { a -> a.assets?.largeImage == "mp:ok" } },
                "the retry must land the asset on the presence",
            )

            // The tick after the successful upload reuses it: one send, no re-upload.
            val sendsBefore = fake.presences.size
            val uploadsBefore = attempts
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t10")
            testScheduler.advanceTimeBy(500)
            testScheduler.runCurrent()
            assertEquals(sendsBefore + 1, fake.presences.size, "the stable tick must send exactly one presence")
            assertEquals(uploadsBefore, attempts, "no re-upload for the cached artwork")
            assertEquals(
                "mp:ok",
                activityOf(fake.presences.size - 1, fake.presences).assets?.largeImage,
                "the stable tick must carry the previously uploaded asset",
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

    /**
     * Regression (stale-artwork invalidation): a remembered asset belongs to the image it
     * was resolved for. When the image changes, the remembered asset must be invalidated
     * even if the new image's upload fails — otherwise every tick of the new song would
     * reuse the previous song's cover in phase 1 and skip phase 2, so the new artwork
     * would never be uploaded or shown.
     */
    @Test
    fun `a remembered asset is never sent for a different image, even if the new upload fails`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            var attempts = 0
            // Image A uploads fine; every attempt for image B fails.
            val connection = connectionWith(fake) { url ->
                attempts++
                if (url.endsWith("a.png")) "mp:cover-a" else null
            }

            // Song A: two-phase send, asset remembered.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t0")
            testScheduler.runCurrent()
            assertEquals(2, fake.presences.size)
            assertEquals("mp:cover-a", activityOf(1, fake.presences).assets?.largeImage)

            // Song B arrives and its upload fails: its presence must NOT carry A's cover.
            connection.setActivity(name = "Song B", largeImage = "http://img.example/b.png", details = "t5")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()
            assertEquals(3, fake.presences.size, "the failed-upload tick must send its own text-only presence")
            assertNull(
                activityOf(2, fake.presences).assets,
                "the new song's phase-1 presence must not carry the previous song's remembered asset",
            )

            // The next tick on the same failed song: the stale asset must still be gone —
            // B keeps no A artwork, and B's upload is retried (cache miss) instead of
            // being skipped by a reused asset.
            val attemptsBefore = attempts
            connection.setActivity(name = "Song B", largeImage = "http://img.example/b.png", details = "t10")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()
            assertEquals(attemptsBefore + 1, attempts, "the failed image B upload must be retried, not skipped by a stale asset")
            assertNull(
                activityOf(3, fake.presences).assets,
                "the stable-failed tick must stay text-only, never fall back to the previous cover",
            )
            assertFalse(
                fake.presences.map { it.activities.first() }
                    .filter { it.name == "Song B" }
                    .any { it.assets?.largeImage == "mp:cover-a" },
                "no presence of the new song may carry the previous song's cover",
            )
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * Regression (real app shape): the app's tick ALWAYS sends the (large + small) pair —
     * the album art plus a fixed icon. The reuse/skip logic must hold for that shape:
     * a stable tick sends exactly one presence carrying BOTH assets with one fetch per
     * image, and a song switch (large changes, small unchanged) keeps phase 1 carrying
     * the remembered small asset while phase 2 resolves the new large image.
     */
    @Test
    fun `a stable large+small tick reuses both assets and a song switch keeps the small asset`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            val fetches = mutableListOf<String>()
            val connection = connectionWith(fake) { url ->
                fetches += url
                when {
                    url.endsWith("a.png") -> "mp:large-a"
                    url.endsWith("b.png") -> "mp:large-b"
                    else -> "mp:icon"
                }
            }

            // The app's real tick shape: album art + fixed icon.
            connection.setActivity(
                name = "Song A",
                largeImage = "http://img.example/a.png",
                smallImage = "http://icon.example/s.png",
                details = "t0",
            )
            testScheduler.runCurrent()
            assertEquals(2, fake.presences.size)
            assertNull(activityOf(0, fake.presences).assets)
            val assets1 = activityOf(1, fake.presences).assets
            assertNotNull(assets1, "phase 2 must carry both assets")
            assertEquals("mp:large-a", assets1?.largeImage)
            assertEquals("mp:icon", assets1?.smallImage)

            // The refresh tick re-sends the SAME pair — one presence, both assets, no re-fetch.
            connection.setActivity(
                name = "Song A",
                largeImage = "http://img.example/a.png",
                smallImage = "http://icon.example/s.png",
                details = "t5",
            )
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()
            assertEquals(3, fake.presences.size, "a stable large+small tick must send exactly one presence")
            val tickAssets = activityOf(2, fake.presences).assets
            assertNotNull(tickAssets, "the stable tick must carry both assets — no image drop")
            assertEquals("mp:large-a", tickAssets?.largeImage)
            assertEquals("mp:icon", tickAssets?.smallImage)
            assertEquals(2, fetches.size, "a stable tick must not re-fetch either image")

            // Song switch: the large image changes, the small icon does not. Phase 1 must
            // already carry the remembered icon (never stripped), phase 2 the new art.
            connection.setActivity(
                name = "Song B",
                largeImage = "http://img.example/b.png",
                smallImage = "http://icon.example/s.png",
                details = "t6",
            )
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()
            assertEquals(5, fake.presences.size, "a new large image takes the two-phase path (small already resolved)")
            val switchPhase1 = activityOf(3, fake.presences).assets
            assertNotNull(switchPhase1, "phase 1 of the song switch must already carry the remembered icon")
            assertNull(switchPhase1?.largeImage, "the new song's phase 1 must not carry the previous song's cover")
            assertEquals("mp:icon", switchPhase1?.smallImage, "the unchanged icon must be reused, not stripped")
            val switchPhase2 = activityOf(4, fake.presences).assets
            assertEquals("mp:large-b", switchPhase2?.largeImage, "phase 2 must carry the newly resolved large image")
            assertEquals("mp:icon", switchPhase2?.smallImage)
            assertEquals(3, fetches.size, "only the new large image may be fetched")
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }

    /**
     * Regression (account change): clearArtworkCache resets the remembered assets, so the
     * next tick of the SAME artwork must re-upload (cache miss, two-phase path) instead of
     * reusing the previous account's assets in phase 1.
     */
    @Test
    fun `clearArtworkCache resets the remembered assets so the next tick re-uploads`() = runTest {
        val previous = DiscordRpc.backgroundDispatcher
        DiscordRpc.backgroundDispatcher = UnconfinedTestDispatcher(testScheduler)
        try {
            ArtworkCache.clear()
            val fake = RecordingGateway()
            var fetchCalls = 0
            val connection = connectionWith(fake) {
                fetchCalls++
                "mp:ok"
            }

            // Song A: two-phase send, asset remembered.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png")
            testScheduler.runCurrent()
            assertEquals(2, fake.presences.size)
            assertEquals("mp:ok", activityOf(1, fake.presences).assets?.largeImage)
            assertEquals(1, fetchCalls)

            // Token/account change: the app clears the cache before reconnecting.
            connection.clearArtworkCache()

            // The same artwork arrives again: it must be re-uploaded, two-phase.
            connection.setActivity(name = "Song A", largeImage = "http://img.example/a.png", details = "t5")
            testScheduler.advanceTimeBy(500) // setActivity's 500 ms min-update-interval debounce (virtual time)
            testScheduler.runCurrent()
            assertEquals(2, fetchCalls, "the cache clear must force a re-upload of the same artwork")
            assertEquals(4, fake.presences.size, "the post-clear tick takes the two-phase path again (text-only then asset)")
            assertNull(
                activityOf(2, fake.presences).assets,
                "the post-clear tick must start text-only — the previous account's asset is not reusable",
            )
            assertEquals("mp:ok", activityOf(3, fake.presences).assets?.largeImage)
            assertEquals(1, ArtworkCache.size, "the re-upload must be cached again")
            connection.closeDirect()
        } finally {
            DiscordRpc.backgroundDispatcher = previous
        }
    }
}
