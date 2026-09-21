package com.metrolist.music.discordrpc

import kotlinx.coroutines.async
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * ArtworkCache in-flight dedup tests: concurrent getOrFetch calls for the same key share
 * ONE fetch (duplicate uploads of the same artwork hit Discord's external-assets rate
 * limit — the app's 5 s refresh tick and the advanced-settings re-sync can both resolve
 * the same URL at once), and a cancelled in-flight fetch releases the slot so the next
 * caller starts a fresh fetch.
 */
class ArtworkCacheInFlightTest {

    @Test
    fun `concurrent getOrFetch calls for the same key share one in-flight fetch`() = runTest {
        ArtworkCache.clear()
        val dispatcher = UnconfinedTestDispatcher(testScheduler)
        val gate = CompletableDeferred<Unit>()
        var fetches = 0

        // The owner enters the fetch and parks on the gate (upload in flight).
        val owner = async(dispatcher) {
            ArtworkCache.getOrFetch("shared-key") {
                fetches++
                gate.await()
                "mp:shared"
            }
        }
        // The joiner arrives while the fetch is in flight (a 5 s tick on the same
        // artwork) — it must NOT start a second fetch.
        val joiner = async(dispatcher) {
            ArtworkCache.getOrFetch("shared-key") {
                fetches++
                "mp:stale"
            }
        }

        gate.complete(Unit)
        testScheduler.runCurrent()

        assertEquals("mp:shared", owner.await(), "the owner must get the fetched result")
        assertEquals("mp:shared", joiner.await(), "the joiner must get the SAME result as the owner")
        assertEquals(1, fetches, "a concurrent call must join the in-flight fetch, not re-fetch")
        assertEquals(1, ArtworkCache.size, "the shared result must be cached exactly once")
    }

    @Test
    fun `a cancelled in-flight fetch releases the slot so the next caller retries`() = runTest {
        ArtworkCache.clear()
        val dispatcher = UnconfinedTestDispatcher(testScheduler)
        val gate = CompletableDeferred<Unit>()
        var fetches = 0

        val owner = async(dispatcher) {
            ArtworkCache.getOrFetch("retry-key") {
                fetches++
                gate.await()
                "mp:never"
            }
        }
        val joiner = async(dispatcher) {
            ArtworkCache.getOrFetch("retry-key") {
                fetches++
                "mp:stale"
            }
        }

        // The owning job is cancelled mid-upload (supersede / clear / close): the
        // joiner must not hang forever on a fetch that will never publish.
        owner.cancel()
        testScheduler.runCurrent()

        // A fresh call after the cancellation must run a NEW fetch (nothing cached,
        // slot released by the owner's finally block).
        val retry = ArtworkCache.getOrFetch("retry-key") {
            fetches++
            "mp:retry"
        }
        assertEquals("mp:retry", retry, "the next call must fetch fresh, not join a dead in-flight slot")
        assertEquals(2, fetches, "only the original (cancelled) fetch and the retry must have run")
        assertEquals(1, ArtworkCache.size, "only the retry's result must be cached")
    }
}
