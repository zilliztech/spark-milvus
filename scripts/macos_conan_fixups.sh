#!/usr/bin/env bash
#
# One-time Conan cache fix-ups needed to build the pinned milvus-storage
# submodule on macOS with a current Xcode toolchain. Safe to re-run.
#
#   1. Accept the installed Apple clang version (Conan's settings.yml stops a
#      few releases behind Xcode).
#   2. Point the libavrocpp / boost recipes at download locations that still
#      exist (same edits as the Dockerfile).
#   3. thrift 0.17.0 (pulled in by arrow/parquet) defines only
#      TEnumIterator::operator!=; libc++ shipped with Xcode 16+ compares the
#      iterators with == in std::map::insert(first, last), so arrow's generated
#      parquet_types.cpp fails to compile. Add the missing operator== to the
#      recipe sources (and to an already-built package, if present).
#
# Usage: scripts/macos_conan_fixups.sh   (after `conan profile detect` and
#        `conan remote add default-conan-local2 ...`)

set -euo pipefail

if ! command -v conan >/dev/null 2>&1; then
    echo "conan not found on PATH (pip install conan==2.25.1)" >&2
    exit 1
fi

CONAN_HOME_DIR="${CONAN_HOME:-$HOME/.conan2}"
REMOTE="${CONAN_REMOTE:-default-conan-local2}"

# ---------------------------------------------------------------------------
# 1. apple-clang version
# ---------------------------------------------------------------------------
clang_major="$(clang --version | sed -n 's/^Apple clang version \([0-9]*\).*/\1/p')"
if [ -n "$clang_major" ] && [ "$clang_major" -gt 17 ]; then
    versions=""
    for v in $(seq 17 "$clang_major"); do
        versions="${versions:+$versions, }\"$v\", \"$v.0\""
    done
    cat > "$CONAN_HOME_DIR/settings_user.yml" <<EOF
compiler:
    apple-clang:
        version: [$versions]
EOF
    echo "settings_user.yml: apple-clang versions up to $clang_major accepted"
fi

# ---------------------------------------------------------------------------
# 2. durable source URLs for avro / boost recipes
# ---------------------------------------------------------------------------
patch_recipe_url() {
    local ref="$1" from="$2" to="$3"
    conan download "$ref" -r "$REMOTE" --only-recipe >/dev/null
    local recipe
    recipe="$(conan cache path "$ref")"
    if grep -Fq "$from" "$recipe/conandata.yml"; then
        sed -i '' "s#${from}#${to}#" "$recipe/conandata.yml"
        echo "patched source URL in $ref"
    fi
}
patch_recipe_url 'libavrocpp/1.12.1.1@milvus/dev#cde7bb587a29f6f233bae7e18b71815d' \
    'https://dlcdn.apache.org/avro/' 'https://archive.apache.org/dist/avro/'
patch_recipe_url 'boost/1.83.0#4e8a94ac1b88312af95eded83cd81ca8' \
    'https://boostorg.jfrog.io/artifactory/main/release/1.83.0/source/' \
    'https://downloads.sourceforge.net/project/boost/boost/1.83.0/'

# ---------------------------------------------------------------------------
# 3. thrift TEnumIterator::operator==
# ---------------------------------------------------------------------------
patch_thrift_header() {
    local header="$1"
    [ -f "$header" ] || return 0
    if grep -q 'bool operator==(const TEnumIterator' "$header"; then
        return 0
    fi
    python3 - "$header" <<'EOF'
import sys
path = sys.argv[1]
src = open(path).read()
old = """  bool operator!=(const TEnumIterator& end) {
    THRIFT_UNUSED_VARIABLE(end);
    assert(end.n_ == -1);
    return (ii_ != n_);
  }
"""
new = old + """
  // libc++ (Xcode 16+) compares iterators with == in std::map::insert(first, last).
  bool operator==(const TEnumIterator& end) {
    THRIFT_UNUSED_VARIABLE(end);
    assert(end.n_ == -1);
    return (ii_ == n_);
  }
"""
if old not in src:
    sys.exit(f"unexpected TEnumIterator layout in {path}")
open(path, "w").write(src.replace(old, new))
print(f"patched {path}")
EOF
}

# Recipe sources (used for any future rebuild) ...
while IFS= read -r header; do
    patch_thrift_header "$header"
done < <(find "$CONAN_HOME_DIR/p" -path '*thrif*' -name Thrift.h 2>/dev/null)

echo "Conan fix-ups applied"
