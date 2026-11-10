#!/usr/bin/env bash
# Put the tools a JVM may spawn ahead of the system ones on a GitHub-hosted
# macOS runner.
#
# The runner has System Integrity Protection disabled, so its platform binaries
# honor DYLD_INSERT_LIBRARIES: a child of a JVM that preloads the arm64
# libjsig.dylib (Hadoop's chmod, ls and rm, bash for group lookups) is refused
# the library and terminates, and Hadoop's local file system cannot create a
# directory. On a Mac with SIP enabled, platform binaries ignore the variable.
# Homebrew's coreutils and bash are not platform binaries, so they load it.
set -euo pipefail
if [[ "$(uname -s)" != Darwin ]]; then
  echo "prepare_macos_jvm_children.sh runs on macOS only" >&2
  exit 1
fi
csrutil status || true
brew list --versions coreutils >/dev/null 2>&1 || brew install coreutils
brew list --versions bash >/dev/null 2>&1 || brew install bash
gnubin="$(brew --prefix coreutils)/libexec/gnubin"
test -x "$gnubin/chmod"
test -x "$(brew --prefix)/bin/bash"
printf '%s\n' "$gnubin" "$(brew --prefix)/bin" >> "${GITHUB_PATH:?}"
