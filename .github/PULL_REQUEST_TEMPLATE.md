<!-- Thanks for the contribution! 🙌
     Fill in what applies to your PR and tick the matching boxes — leave the rest unchecked (don't delete them). -->

## 📋 What are you changing?
<!-- The problem, your solution, context and decisions. If similar work already exists, link it and explain how this PR differs. -->

## 🔗 Related issues
<!-- Avoid words that auto-close the issue ("fixes", "closes") — use the `issue https://...` format. -->
- Issue: `issue https://github.com/N-Zik-Group/DiscordRPC/issues/…`

## 🚀 Type of change
- [ ] ✨ New feature (`feat`)
- [ ] 🐛 Bug fix (`fix`)
- [ ] 📈 Improvement of existing behavior (`improve`)
- [ ] ⚡ Performance (`perf`)
- [ ] 🧹 Refactor, no behavior change (`refactor`)
- [ ] 🧪 Tests only (`test`)
- [ ] 📖 Docs only (`docs`)
- [ ] 🛠️ Build / tooling / deps (`chore`)

## 🤖 Made with AI?
<!-- Just tells us whether an AI agent helped produce this change. -->
- [ ] 🤖 Yes — an AI agent helped produce this change
- [ ] 👤 No — I wrote it myself, no AI assistance

## ✅ How can we verify it?
<!-- Steps for a reviewer + concrete evidence: diffs, test output. "It works" alone doesn't cut it 🙂 -->
1.
2.

---

## 🛠️ Checklist
> Tick every box that applies to your PR; leave the rest unchecked.

### ✅ Always (every PR)
- [ ] I tested this change myself before opening the PR
- [ ] No force push, no rewritten history
- [ ] Branch named `feat/…`, `fix/…` or `chore/…`
- [ ] Commits follow the repo convention — e.g. `fix(rpc): dedup in-flight artwork uploads`
  <!-- `type(scope): description` · English · imperative · under 72 chars · no final period · issue links as `issue https://...` -->

### 🏗️ Code
- [ ] Coroutines are properly scoped — no `GlobalScope`, no fire-and-forget work left running after the connection is closed
- [ ] I log with `Timber.tag(...)` only — no `println`
- [ ] New behavior includes unit tests under `src/test/kotlin/`
- [ ] I didn't add new dependencies without listing them in the PR description
- [ ] No secrets or tokens in the diff

### 🧪 Build & tests
- [ ] The module compiles and its unit tests are green

## 🗒️ Anything else?
-
