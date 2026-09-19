#!/usr/bin/env bash
#
# macOS counterpart of milvus-storage/java/patch_native_runpath.sh.
#
# NativeLibraryLoader extracts every bundled library under
# native/darwin-<arch>/ into one flat temp directory (subdirectories such as
# ossl-modules/ are preserved) and then System.load()s
# libmilvus-storage-jni.dylib. For dyld to resolve the sibling libraries from
# that directory, every dylib must:
#
#   1. reference its dependencies through @rpath/<name> (Conan already does
#      this for the packages it builds, but anything still pointing at an
#      absolute build/Conan-cache path is rewritten here), and
#   2. carry an LC_RPATH entry that points back at its own directory
#      (@loader_path, or @loader_path/.. for nested libraries).
#
# install_name_tool invalidates the ad-hoc code signature that the linker
# attached, and dyld on Apple Silicon refuses to load an image whose
# signature no longer matches, so every modified library is re-signed.

set -euo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <native-library-directory>" >&2
    exit 2
fi

NATIVE_DIR="$1"

if [ ! -d "$NATIVE_DIR" ]; then
    echo "Native library directory not found: $NATIVE_DIR" >&2
    exit 1
fi

# A Mach-O file starts with one of these magic numbers (64-bit thin, 32-bit
# thin, or a fat/universal binary), read as big-endian hex. Anything else under
# the directory (linker scripts, text placeholders in build tests) is skipped,
# and a directory with no Mach-O library at all is not an error: the Linux
# Makefile path never reaches this script, but the Makefile's own tests exercise
# the Darwin selection with placeholder files on every host.
is_macho() {
    local magic
    magic="$(head -c 4 "$1" 2>/dev/null | od -An -tx1 | tr -d ' \n')"
    case "$magic" in
        cffaedfe|feedfacf|cefaedfe|feedface|cafebabe|bebafeca) return 0 ;;
        *) return 1 ;;
    esac
}

macho_count=0
while IFS= read -r -d '' library; do
    if is_macho "$library"; then
        macho_count=$((macho_count + 1))
    fi
done < <(find "$NATIVE_DIR" -type f -name '*.dylib' -print0)

if [ "$macho_count" -eq 0 ]; then
    echo "No Mach-O shared libraries found in $NATIVE_DIR; nothing to patch"
    exit 0
fi

for tool in otool install_name_tool codesign; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "$tool is required (install the Xcode Command Line Tools)" >&2
        exit 1
    fi
done

# Collect the basenames that are shipped so absolute references to them can
# be rewritten into @rpath references. (A plain newline-separated list: the
# system bash on macOS is 3.2 and has no associative arrays.)
shipped="$(find "$NATIVE_DIR" -type f -name '*.dylib' -exec basename {} \;)"
is_shipped() {
    printf '%s\n' "$shipped" | grep -Fxq -- "$1"
}

patched=0
while IFS= read -r -d '' library; do
    if ! is_macho "$library"; then
        echo "Skipping non-Mach-O file: $library"
        continue
    fi

    relative_path="${library#"$NATIVE_DIR"/}"
    relative_dir="$(dirname "$relative_path")"
    runpath='@loader_path'
    while [ "$relative_dir" != "." ]; do
        runpath="$runpath/.."
        relative_dir="$(dirname "$relative_dir")"
    done

    args=()

    # The install name itself should be @rpath-relative so that consumers
    # linked against this copy record a relocatable reference.
    current_id="$(otool -D "$library" | sed -n '2p')"
    if [ -n "$current_id" ] && [[ "$current_id" != @rpath/* ]] && [[ "$current_id" != @loader_path/* ]]; then
        args+=(-id "@rpath/$(basename "$library")")
    fi

    # Rewrite absolute dependency paths that resolve to a shipped library.
    while IFS= read -r dep; do
        [ -z "$dep" ] && continue
        case "$dep" in
            /usr/lib/*|/System/*|@rpath/*|@loader_path/*|@executable_path/*) continue ;;
        esac
        dep_name="$(basename "$dep")"
        if is_shipped "$dep_name"; then
            args+=(-change "$dep" "@rpath/$dep_name")
        fi
    done < <(otool -L "$library" | tail -n +2 | awk '{print $1}')

    # Drop rpaths that point into the build tree / Conan cache and add the
    # relocatable one (once).
    existing_rpaths="$(otool -l "$library" | awk '/LC_RPATH/{f=1} f&&/path /{print $2; f=0}')"
    has_runpath=0
    while IFS= read -r rp; do
        [ -z "$rp" ] && continue
        if [ "$rp" = "$runpath" ]; then
            has_runpath=1
        elif [[ "$rp" == /* ]]; then
            args+=(-delete_rpath "$rp")
        fi
    done <<< "$existing_rpaths"
    if [ "$has_runpath" -eq 0 ]; then
        args+=(-add_rpath "$runpath")
    fi

    if [ "${#args[@]}" -gt 0 ]; then
        chmod u+w "$library"
        install_name_tool "${args[@]}" "$library" 2> >(grep -v 'invalidate the code signature' >&2 || true)
    fi
    codesign --force --sign - "$library" 2> >(grep -v 'replacing existing signature' >&2 || true)
    patched=$((patched + 1))
done < <(find "$NATIVE_DIR" -type f -name '*.dylib' -print0)

echo "Patched rpath and re-signed $patched bundled shared libraries in $NATIVE_DIR"
