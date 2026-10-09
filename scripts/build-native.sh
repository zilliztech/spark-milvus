#!/usr/bin/env bash
# Build both upstream JNI implementations against one Conan host graph.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec python3 "$root/native-build/build.py" "$@"
