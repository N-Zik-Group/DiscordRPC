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

    /**
     * Application id used by BOTH the RPC activity payload (`application_id`) and the
     * external-assets upload (`POST /api/v9/applications/{id}/external-assets`). Single
     * source for the whole module and its host app (item 11 — the two historical ids,
     * 1411019391843172514 in the module and 1379051016007454760 in the app, are unified
     * on the user-facing app id).
     */
    const val APPLICATION_ID = "1379051016007454760"
}
