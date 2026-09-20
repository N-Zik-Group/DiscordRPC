package com.metrolist.music.discordrpc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import timber.log.Timber

/**
 * Locks down DiscordRpcConnection.httpScopeExceptionHandler (issue #606): an unhandled
 * failure in the httpScope is routed to a Timber log instead of an unhandled-exception
 * crash path, and a failing coroutine can't cancel its siblings (SupervisorJob root,
 * same pattern as Invidious.scope).
 */
class DiscordRpcHttpScopeHandlerTest {

    private val capturedLogs = mutableListOf<Triple<String?, String, Throwable?>>()

    private val captureTree = object : Timber.Tree() {
        override fun log(priority: Int, tag: String?, message: String, t: Throwable?) {
            capturedLogs.add(Triple(tag, message, t))
        }
    }

    @AfterEach
    fun tearDown() {
        Timber.uproot(captureTree)
        capturedLogs.clear()
    }

    @Test
    fun `a failure is logged by the httpScope handler and siblings survive`() = runTest {
        Timber.plant(captureTree)

        val scope = CoroutineScope(
            SupervisorJob() +
                DiscordRpcConnection.httpScopeExceptionHandler +
                UnconfinedTestDispatcher()
        )
        val siblingRan = CountDownLatch(1)

        val failing = scope.launch { error("simulated httpScope failure") }
        failing.join()
        assertTrue(failing.isCancelled, "failing child must have run and failed")

        scope.launch { siblingRan.countDown() }
        assertTrue(siblingRan.await(5, TimeUnit.SECONDS), "sibling must still run after a sibling failure")

        val (tag, message, throwable) = capturedLogs.single()
        assertEquals("DiscordRpc", tag)
        // Timber.e(throwable, message) appends the stack trace to the message handed to trees.
        assertTrue(
            message.startsWith("httpScope coroutine failed"),
            "log message must start with the handler message, was: $message"
        )
        assertNotNull(throwable)
        assertTrue(throwable is IllegalStateException)

        scope.cancel()
    }
}
