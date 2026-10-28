#!/usr/bin/env bash
# Prepare a public GitHub-hosted runner for the checked-in native build profiles:
# Ubuntu 22.04 for the Linux platforms, macOS 15 for darwin-aarch64.
set -euo pipefail

platform="${1:?usage: prepare_native.sh linux-x86_64|linux-aarch64|darwin-aarch64}"
case "$(uname -s):$(uname -m):$platform" in
  Linux:x86_64:linux-x86_64|Linux:aarch64:linux-aarch64|Darwin:arm64:darwin-aarch64) ;;
  *) echo "Runner does not match native platform $platform" >&2; exit 1 ;;
esac

# The Linux images carry SDKs this job never uses; freeing them comes before the
# disk check. Never remove local developer tooling or caches, and never apply
# this cleanup to a self-hosted runner. The macOS images have no such bulk.
if [[ "$(uname -s)" == Linux && "${GITHUB_ACTIONS:-}" == "true" && "${RUNNER_ENVIRONMENT:-}" == "github-hosted" ]]; then
  sudo rm -rf -- /usr/local/lib/android /usr/share/dotnet /opt/ghc
fi
runner_directory="${RUNNER_TEMP:?RUNNER_TEMP must name the disposable runner directory}"
available_kib="$(df -Pk "$runner_directory" | awk 'NR == 2 { print $4 }')"
if [[ ! "$available_kib" =~ ^[0-9]+$ ]] || (( available_kib < 30 * 1024 * 1024 )); then
  echo "Native builds need at least 30 GiB free before restoring dependency caches; available: ${available_kib:-unknown} KiB" >&2
  df -h "$runner_directory" >&2
  exit 1
fi

# Rust and Java are pinned by the repository, not taken from the runner image:
# the image rolls weekly and every patch bump changes the native cache key.
pins="$(dirname "${BASH_SOURCE[0]}")/../../native/build/toolchain.json"
rust_version="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["rust"])' "$pins")"
java_version="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["java"])' "$pins")"
rustup toolchain install "$rust_version" --profile minimal --no-self-update
rustup default "$rust_version"
case "$(rustc --version)" in
  "rustc $rust_version "*) ;;
  *) echo "rustup did not select Rust $rust_version: $(rustc --version)" >&2; exit 1 ;;
esac
case "$("${JAVA_HOME:?}/bin/java" -version 2>&1 | head -1)" in
  *"version \"$java_version\""*) ;;
  *) echo "JAVA_HOME must provide Java $java_version from native/build/toolchain.json: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)" >&2; exit 1 ;;
esac

toolchain="$runner_directory/native-toolchain"
python3 -m venv "$toolchain"


if [[ "$(uname -s)" == Darwin ]]; then
  # The darwin profiles were validated with Apple clang 17, which is what the
  # runner's default Xcode (16.4) and its Xcode 26 installs both report, and
  # they name CMake 3.31.10 as a platform tool. The OpenMP runtime Apple clang
  # lacks is a Conan dependency (llvm-openmp), built for the deployment target
  # like everything else: a Homebrew bottle would state the runner's own macOS
  # release. ccache is not preinstalled.
  brew list --versions ccache >/dev/null 2>&1 || brew install ccache
  "$toolchain/bin/python" -m pip install --disable-pip-version-check \
    conan==2.25.1 cmake==3.31.10 ninja==1.13.0
  export PATH="$toolchain/bin:$PATH"
  printf '%s\n' "$toolchain/bin" >> "${GITHUB_PATH:?}"
  # Every library is built for this deployment target; the ABI audit and the
  # cache key hold the bundle to the same value.
  printf 'MACOSX_DEPLOYMENT_TARGET=14.0\n' >> "${GITHUB_ENV:?}"
  export MACOSX_DEPLOYMENT_TARGET=14.0
  # bindgen (the storage Rust bridge) loads libclang from the Command Line Tools.
  printf 'LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib\n' >> "$GITHUB_ENV"
  clang_version="$(clang --version | head -1)"
  case "$clang_version" in
    "Apple clang version 17."*) ;;
    *) echo "The darwin profiles expect Apple clang 17; the selected Xcode reports: $clang_version" >&2; exit 1 ;;
  esac
  xcode-select -p
  xcrun --show-sdk-version
  for tool in otool install_name_tool codesign; do command -v "$tool" >/dev/null; done
  test -f /Library/Developer/CommandLineTools/usr/lib/libclang.dylib
  test -f "${JAVA_HOME:?}/lib/libjsig.dylib"
else
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends \
    gcc-12 g++-12 gfortran-12 libaio-dev libclang-dev patchelf binutils \
    ccache pkg-config automake autoconf libtool ninja-build
  "$toolchain/bin/python" -m pip install --disable-pip-version-check \
    conan==2.25.1 cmake==3.27.5 ninja==1.13.0
  export PATH="$toolchain/bin:$PATH"
  printf '%s\n' "$toolchain/bin" >> "${GITHUB_PATH:?}"
fi

conan profile detect --force
conan remote add --force default-conan-local2 \
  https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2
test -x "${JAVA_HOME:?}/bin/javac"
rustc --version
cargo --version
cmake --version
conan --version
ccache --version | head -1
df -h "$RUNNER_TEMP"
