package com.metrolist.music.discordrpc

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers

/**
 * Injection seam (issue #606, Goal G5): this module can't depend on the app's centralized
 * NzikDispatchers (Gradle dependency direction is app -> this module, never the reverse).
 *
 * Defaults to Dispatchers.IO so the module works standalone (its own unit tests, or a host
 * app that never overrides the seam); the gateway loop is ~100% socket waits, so IO is
 * sufficient for both the gateway scope and the HTTP scope. The app wires this to
 * NzikDispatchers.DATA once at startup in MainApplication.onCreate — same single-write-at-
 * startup, read-from-background-threads reason as Invidious.backgroundDispatcher (hence
 * @Volatile).
 */
object DiscordRpc {

    @Volatile
    var backgroundDispatcher: CoroutineDispatcher = Dispatchers.IO
}
