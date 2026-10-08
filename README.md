<div align="center">
  <img alt="project's banner" src="https://raw.githubusercontent.com/N-Zik-Group/N-Zik/main/assets/design/ic_banner2.png" width="1080" />

  <h1>DiscordRPC-Android</h1>
  <p>
    A standalone Discord Rich Presence (RPC) client for Android, built in Kotlin.
    Used as a Gradle module in <a href="https://github.com/N-Zik-Group/N-Zik">N-Zik</a>.
  </p>
  <p>
    <strong>Note:</strong> This project is a fork of the Discord RPC implementation
    originally created by <a href="https://github.com/MetrolistGroup/Metrolist">MetroList</a>.
  </p>
</div>

<br>

<div align="center">
  
  [![License: GPL v3](https://img.shields.io/github/license/N-Zik-Group/DiscordRPC?color=blue)](https://www.gnu.org/licenses/gpl-3.0)
  [![CodeFactor](https://www.codefactor.io/repository/github/n-zik-group/discordrpc/badge)](https://www.codefactor.io/repository/github/n-zik-group/discordrpc)
  
</div>

<div align="center">

## 📚 Wiki

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/N-Zik-Group/DiscordRPC)

<br>

## 🌍 Community

Join the N-Zik Discord:

<a href="https://discord.gg/bneHC7QRje">
  <img src="https://discord.com/api/guilds/1345079801324634193/widget.png?style=banner2" alt="Discord Server">
</a>

<br>

</div>

# 🎧 Features

- Connects to Discord's Gateway via WebSockets
- Handles Rich Presence (Activity) updates
- Built with Kotlin and Ktor

# 📜 Integration

This library is designed to be added as a submodule in Android projects.

```groovy
// settings.gradle.kts
include(":discordrpc")

// build.gradle.kts (app)
implementation(projects.discordrpc)
```

# 🤝 Contributing

## 🛠️ Improve the Module

Pull requests are welcome!
Feel free to fix bugs, enhance features, or suggest new ideas.

# 📦 Clone the repo

Use this command to clone the repo

```
git clone https://github.com/N-Zik-Group/DiscordRPC.git
```

# 🫂 Acknowledgements

### 🛠 Based on / Inspired by:

- [**MetroList**](https://github.com/MetrolistGroup/Metrolist): The Discord RPC implementation this project is based on.

Made with ❤️ by [NEVARLeVrai](https://github.com/NEVARLeVrai)
Licensed under GPLv3 - see [LICENSE](LICENSE)
