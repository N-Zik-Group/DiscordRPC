package com.metrolist.music.discordrpc

import java.util.concurrent.ConcurrentHashMap

internal object ArtworkCache {

    /**
     * Hard cap on the cached image resolutions (upstream parity:
     * DiscordExternalAssets.CACHE_MAX_SIZE = 128). [trimCache] evicts on insert, so the
     * cache never grows past the cap.
     */
    private const val MAX_ENTRIES = 128

    private val cache = ConcurrentHashMap<String, String>()

    /** Number of cached entries (test support — the cap is otherwise unobservable). */
    internal val size: Int get() = cache.size

    suspend fun getOrFetch(key: String, fetch: suspend () -> String?): String? {
        val cached = cache[key]
        if (cached != null) return cached
        val result = fetch()
        if (result != null) {
            cache[key] = result
            trimCache()
            return result
        }
        return null
    }

    /**
     * Upstream parity (DiscordExternalAssets.trimCache L82-87): evict insertions above
     * the cap. Note: ConcurrentHashMap.keys has no order guarantee — the upstream does
     * the same; the requirement is the cap itself, not which entries survive.
     */
    private fun trimCache() {
        if (cache.size > MAX_ENTRIES) {
            val toRemove = cache.size - MAX_ENTRIES
            cache.keys.take(toRemove).forEach { cache.remove(it) }
        }
    }

    /**
     * Empties the cache. Fork addition (item 10): the app calls this on token change, so
     * a new account never inherits the previous account's uploaded asset mappings
     * (upstream never calls clearCache() in production).
     */
    fun clear() {
        cache.clear()
    }
}
