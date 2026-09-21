# Consumer rules for the discordrpc module (item 16).
#
# The upstream block this mirrors (Metrolist app/proguard-rules.pro L184-188) keeps
# DiscordRpcManager + its JNI `native <methods>` — there is no JNI layer in this fork,
# so the keep is restricted to the module's PUBLIC API that the host app links against.
# Internal classes (ArtworkCache, RefusalHolder, the test seams) are not kept.

## Public API surface
-keep class com.metrolist.music.discordrpc.DiscordRpc { *; }
-keep class com.metrolist.music.discordrpc.DiscordRpcConnection { *; }
-keep class com.metrolist.music.discordrpc.GatewayWebSocket { *; }
-keep class com.metrolist.music.discordrpc.ActivityType { *; }
-keep class com.metrolist.music.discordrpc.SuperProperties { *; }
-keep class com.metrolist.music.discordrpc.UserInfo { *; }
-keep class com.metrolist.music.discordrpc.entities.** { *; }

## Top-level public entry point (ExternalAssets.kt -> fetchExternalAsset)
-keep class com.metrolist.music.discordrpc.ExternalAssetsKt {
    public static *;
}

## kotlinx-serialization: @Serializable metadata + @SerialName survive minification
## (upstream parity: app/proguard-rules.pro L190-191)
-keepattributes *Annotation*
