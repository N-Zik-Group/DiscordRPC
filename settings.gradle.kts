// Standalone build settings (root project = this repository).
// When this repository is used as a git submodule inside N-Zik, this file is
// NOT used — the module is built as a subproject of N-Zik's root build.

pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
    plugins {
        id("com.android.library") version "9.4.1"
        id("org.jetbrains.kotlin.android") version "2.4.20"
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "DiscordRPC"
