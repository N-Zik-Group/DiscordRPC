plugins {
    id("com.android.library")
    kotlin("android")
    alias(libs.plugins.kotlin.serialization)
}

configure<com.android.build.api.dsl.LibraryExtension> {
    namespace = "com.metrolist.music.discordrpc"
    compileSdk = 37

    defaultConfig {
        minSdk = 24
    }

    compileOptions {
        isCoreLibraryDesugaringEnabled = true
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
}

dependencies {
    implementation(libs.ktor.core)
    implementation(libs.ktor.okhttp)
    implementation(libs.ktor.content.negotiation)
    implementation(libs.ktor.serialization.json)
    implementation(libs.ktor.encoding)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.timber)
    
    // Fallback for org.json (if android.jar doesn't provide it during tests)
    compileOnly("org.json:json:20231013")

    coreLibraryDesugaring(libs.desugaring.nio)
}
