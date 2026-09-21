// The UploadEngine below implements ktor's HttpClientEngine, which is @InternalAPI in Ktor 3
// (engine implementations are an internal extension point). The opt-in is test-only:
// production code never implements an engine.
@file:OptIn(InternalAPI::class)

package com.metrolist.music.discordrpc

import io.ktor.client.HttpClient
import io.ktor.client.engine.HttpClientEngine
import io.ktor.client.engine.HttpClientEngineConfig
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpProtocolVersion
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.util.date.GMTDate
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.InternalAPI
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

/**
 * Real upload path tests (review gap, items 4/11): [fetchExternalAsset] is the production
 * external-asset upload — the two-phase tests cover it only through the `externalAssetFetcher`
 * test seam, so the cancellable contract of the REAL function (a CancellationException
 * propagates, the request never completes, nothing is cached) and the unified application id
 * in the upload URL are pinned here against a scripted Ktor engine.
 */
class ExternalAssetUploadTest {

    /** Scripted engine: records the request URL, parks/answers the POST behind a lambda. */
    private class UploadEngine(
        private val onExecute: suspend (HttpRequestData) -> HttpResponseData,
    ) : HttpClientEngine {
        var lastUrl: String? = null
        var responded = false
        // Ktor derives the engine's closed state from the Job in its coroutine context.
        private val job = SupervisorJob()

        override val coroutineContext: CoroutineContext = Dispatchers.Unconfined + job
        override val dispatcher: CoroutineDispatcher = Dispatchers.Unconfined
        override val config: HttpClientEngineConfig = HttpClientEngineConfig()

        override fun close() {
            job.cancel()
        }

        override suspend fun execute(request: HttpRequestData): HttpResponseData {
            lastUrl = request.url.toString()
            return onExecute(request).also { responded = true }
        }
    }

    // Ktor requires a Job in the response call context (it structures the response
    // processing); a bare EmptyCoroutineContext fails inside the pipeline.
    private val responseCallContext: CoroutineContext = Dispatchers.Unconfined + SupervisorJob()

    private fun jsonResponse(body: String): HttpResponseData = HttpResponseData(
        statusCode = HttpStatusCode.OK,
        requestTime = GMTDate(),
        headers = headersOf(
            HttpHeaders.ContentType to listOf("application/json"),
            // Without an explicit length the client pipeline reads an empty body.
            HttpHeaders.ContentLength to listOf(body.length.toString()),
        ),
        version = HttpProtocolVersion.HTTP_1_1,
        // A real engine hands back a ByteReadChannel; a bare ByteArray is read as an
        // empty body by the client pipeline.
        body = ByteReadChannel(body.toByteArray()),
        callContext = responseCallContext,
    )

    @Test
    fun `a cancelled upload propagates the CancellationException and the request never completes`() = runTest {
        val gate = CompletableDeferred<Unit>()
        val engine = UploadEngine {
            gate.await() // the upload stays in flight until cancelled
            jsonResponse("""[{"external_asset_path":"abc"}]""")
        }
        val client = HttpClient(engine)

        val job = launch(UnconfinedTestDispatcher(testScheduler)) {
            fetchExternalAsset(client, DiscordRpc.APPLICATION_ID, "t", "http://img.example/a.png", "ua")
        }
        testScheduler.runCurrent()

        // Item 11: the upload targets the unified application id.
        assertEquals(
            "https://discord.com/api/v9/applications/${DiscordRpc.APPLICATION_ID}/external-assets",
            engine.lastUrl,
            "the upload URL must carry the module's APPLICATION_ID",
        )
        assertEquals("1379051016007454760", DiscordRpc.APPLICATION_ID, "item 11 pins the unified application id")

        job.cancel()
        testScheduler.runCurrent()

        // If the CancellationException were swallowed by the generic catch, the job would
        // complete normally (with null) instead of ending cancelled.
        assertFalse(job.isActive)
        assertTrue(job.isCancelled, "a CancellationException must propagate out of the upload")
        assertFalse(engine.responded, "a cancelled upload must never complete the HTTP request")
    }

    @Test
    fun `the real upload path resolves to the mp asset through the scripted engine`() = runTest {
        val engine = UploadEngine {
            jsonResponse("""[{"external_asset_path":"abc"}]""")
        }
        val client = HttpClient(engine)

        val asset = fetchExternalAsset(client, DiscordRpc.APPLICATION_ID, "t", "http://img.example/a.png", "ua")
        testScheduler.runCurrent()

        assertEquals("mp:abc", asset, "a successful upload must yield the mp: asset path")
        assertEquals(
            "https://discord.com/api/v9/applications/${DiscordRpc.APPLICATION_ID}/external-assets",
            engine.lastUrl,
            "the upload URL must carry the module's APPLICATION_ID",
        )
    }
}
