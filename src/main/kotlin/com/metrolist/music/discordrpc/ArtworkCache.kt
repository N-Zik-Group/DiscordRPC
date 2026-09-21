package com.metrolist.music.discordrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.ConcurrentHashMap

internal object ArtworkCache {

    /**
     * Hard cap on the cached image resolutions (upstream parity:
     * DiscordExternalAssets.CACHE_MAX_SIZE = 128). [trimCache] evicts on insert, so the
     * cache never grows past the cap.
     */
    private const val MAX_ENTRIES = 128

    private val cache = ConcurrentHashMap<String, String>()

    /**
     * In-flight fetch per key: concurrent [getOrFetch] calls for the same image share ONE
     * fetch instead of each uploading the same artwork (concurrent duplicate uploads hit
     * Discord's external-assets rate limit — the app's 5 s refresh tick and the advanced
     * settings re-sync can both resolve the same URL at once). The owner publishes the
     * outcome to the joiners and releases the slot on completion, cancellation or failure.
     */
    private val inFlight = ConcurrentHashMap<String, CompletableDeferred<String?>>()

    /** Number of cached entries (test support — the cap is otherwise unobservable). */
    internal val size: Int get() = cache.size

    /**
     * Returns the cached resolution for [key], or runs [fetch] once and caches a non-null
     * result. A null result (failed upload) is NEVER cached, so the next call retries the
     * fetch. Concurrent callers for the same key join the single in-flight fetch instead
     * of duplicating it.
     */
    suspend fun getOrFetch(key: String, fetch: suspend () -> String?): String? {
        val cached = cache[key]
        if (cached != null) return cached

        val deferred = CompletableDeferred<String?>()
        val alreadyRunning = inFlight.putIfAbsent(key, deferred)

        return if (alreadyRunning == null) {
            // Owner of this key's fetch: run it, cache a success, publish to the joiners,
            // then release the slot.
            try {
                val result = fetch()
                if (result != null) {
                    cache[key] = result
                    trimCache()
                }
                deferred.complete(result)
                result
            } catch (e: CancellationException) {
                // The owning job went away (supersede / clear / close): the joiners must
                // not hang on a fetch that will never publish, and the next caller starts
                // a FRESH fetch (nothing is cached on cancellation).
                deferred.cancel(e)
                throw e
            } finally {
                inFlight.remove(key, deferred)
            }
        } else {
            // A fetch for this key is already in flight: join it instead of uploading the
            // same image again (duplicate uploads trigger Discord 429s).
            alreadyRunning.await()
        }
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
