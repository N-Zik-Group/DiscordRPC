package com.metrolist.music.discordrpc

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * ArtworkCache bound tests (item 10): the cache never grows past 128 entries (eviction on
 * insertion, upstream trimCache parity) and clear() empties it (fork addition, wired to the
 * token change on the app side).
 */
class ArtworkCacheCapTest {

    @Test
    fun `the cache never grows past 128 entries`() = runTest {
        ArtworkCache.clear()
        repeat(129) { i ->
            ArtworkCache.getOrFetch("key-$i") { "mp:value-$i" }
        }
        assertTrue(
            ArtworkCache.size <= 128,
            "inserting above the cap must evict down to the cap (size=${ArtworkCache.size})",
        )
        // Spec constraint: the test asserts the bound only — the eviction order of
        // ConcurrentHashMap.keys is not guaranteed (upstream trimCache parity), so no
        // specific surviving key is asserted.
    }

    @Test
    fun `clear empties the cache`() = runTest {
        ArtworkCache.clear()
        repeat(10) { i ->
            ArtworkCache.getOrFetch("key-$i") { "mp:value-$i" }
        }
        assertEquals(10, ArtworkCache.size)
        ArtworkCache.clear()
        assertEquals(0, ArtworkCache.size, "clear() must empty the cache")
    }

    @Test
    fun `a null fetch result is not cached`() = runTest {
        ArtworkCache.clear()
        var fetches = 0
        val first = ArtworkCache.getOrFetch("key-null") {
            fetches++
            null
        }
        val second = ArtworkCache.getOrFetch("key-null") {
            fetches++
            null
        }
        assertNull(first)
        assertNull(second)
        assertEquals(2, fetches, "a null result must not be served from the cache on the next call")
        assertEquals(0, ArtworkCache.size, "a null resolution must not occupy a cache slot")
    }
}
