#!/usr/bin/env bash
# Build the root Knowhere submodule or import its historical JNI package for local verification.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/build-knowhere.sh build [--jobs N] [--with-cardinal] [--cardinal-repository REPOSITORY]
       scripts/build-knowhere.sh import-ci [--artifact-dir DIRECTORY]

build compiles the revision fixed by the root knowhere Git submodule, runs the
upstream C/JNI tests, and packages its native dependencies. Install the upstream
prerequisites first: Linux, GCC/G++/Fortran 12, Conan 2, CMake 3, Maven, JDK 11+,
Python 3, patchelf, binutils, libaio development headers and autotools.

import-ci imports the historical 9dc2b8ad PR #1829 CI package after checking
source provenance and artifact checksums. It is available only when the root
submodule is checked out at that historical revision; current revisions must be
built. --artifact-dir reuses an already extracted CI artifact.
Both commands require JAVA_HOME and keep artifacts, licenses and verification
logs under target/knowhere-native/<revision>/<platform>. Nothing is published or
installed into Maven. The current upstream package has missing license material;
it is suitable for local testing only until that upstream report is resolved.

--with-cardinal enables the upstream enterprise engine and writes a separate
<revision>/cardinal/<platform> artifact. Both Cardinal revisions are read from
the pinned Knowhere CMake files and checked out in independent build clones.
--cardinal-repository can point to an authorized local clone, used read-only;
otherwise the upstream Cardinal repository is used and requires access.
USAGE
}

fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }
require() { command -v "$1" >/dev/null || fail "Required tool not found: $1"; }
verify_sha() { printf '%s  %s\n' "$1" "$2" | sha256sum --check --status || fail "SHA-256 mismatch: $2"; }

action=${1:---help}
case "$action" in
  -h|--help|help) usage; exit 0 ;;
  build|import-ci) shift ;;
  *) usage >&2; exit 2 ;;
esac
jobs=2
artifact_dir=
with_cardinal=false
cardinal_repository=https://github.com/zilliztech/cardinal.git
while (($#)); do
  case "$1" in
    --jobs) [[ $# -ge 2 && $action == build ]] || fail 'Use --jobs N with build'; jobs=$2; shift 2 ;;
    --artifact-dir) [[ $# -ge 2 && $action == import-ci ]] || fail 'Use --artifact-dir DIRECTORY with import-ci'; artifact_dir=$2; shift 2 ;;
    --with-cardinal) [[ $action == build ]] || fail 'Use --with-cardinal with build'; with_cardinal=true; shift ;;
    --cardinal-repository) [[ $# -ge 2 && $action == build ]] || fail 'Use --cardinal-repository REPOSITORY with build'; cardinal_repository=$2; shift 2 ;;
    *) fail "Unknown argument: $1" ;;
  esac
done
[[ $jobs =~ ^[1-9][0-9]*$ && $jobs -le 32 ]] || fail 'Use between 1 and 32 build jobs'
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
require git
gitmodules=$root/.gitmodules
[[ -f $gitmodules ]] || fail "Missing submodule configuration: $gitmodules"
git -C "$root" ls-files --error-unmatch -- .gitmodules >/dev/null 2>&1 || \
  fail 'The root .gitmodules file must be version controlled'
mapfile -t repositories < <(git -C "$root" config --file "$gitmodules" --get-all submodule.knowhere.url 2>/dev/null || true)
((${#repositories[@]} == 1)) || fail 'Tracked .gitmodules must contain exactly one submodule.knowhere.url'
repository=${repositories[0]}
[[ $repository =~ ^https://[^[:space:]]+$ && ! $repository =~ ^https://[^/@[:space:]]+@ ]] || \
  fail 'submodule.knowhere.url must be a non-empty HTTPS URL without user information'
submodule=$root/knowhere
[[ -d $submodule && -e $submodule/.git ]] || fail 'Initialize the root knowhere Git submodule first'
read -r gitlink_mode revision gitlink_stage gitlink_path < <(git -C "$root" ls-files --stage -- knowhere)
[[ $gitlink_mode == 160000 && $gitlink_stage == 0 && $gitlink_path == knowhere && $revision =~ ^[a-f0-9]{40}$ ]] || \
  fail 'Knowhere must be recorded as one Git submodule entry'
[[ $(git -C "$submodule" rev-parse HEAD) == "$revision" ]] || \
  fail 'Knowhere submodule HEAD differs from the recorded gitlink'
[[ $revision =~ ^[a-f0-9]{40}$ ]] || fail 'Invalid Knowhere submodule revision'
[[ $(uname -s) == Linux ]] || fail 'Upstream platform packaging currently supports Linux only'
case "$(uname -m)" in
  x86_64) platform=linux-x86_64 ;;
  aarch64|arm64) platform=linux-aarch64 ;;
  *) fail 'Unsupported machine architecture' ;;
esac
[[ -n ${JAVA_HOME:-} && -x $JAVA_HOME/bin/java && -x $JAVA_HOME/bin/javac ]] || fail 'Set JAVA_HOME to a JDK 11 or later'
[[ -f $JAVA_HOME/lib/libjsig.so ]] || fail 'The selected JDK must provide lib/libjsig.so for HotSpot signal chaining'
export PATH="$JAVA_HOME/bin:$PATH"
require sha256sum
require unzip
require python3
variant=
if [[ $with_cardinal == true ]]; then variant=/cardinal; fi
output=$root/target/knowhere-native/$revision$variant/$platform
mkdir -p "$output"
verification=$(mktemp -d "$output/verification-XXXXXXXX")
exec > >(tee "$verification/run.log") 2>&1
printf 'Knowhere revision: %s\nKnowhere repository: %s\nPlatform: %s\nVerification: %s\n' \
  "$revision" "$repository" "$platform" "$verification"

if [[ $action == import-ci ]]; then
  require gh
  require jq
  [[ $revision == 9dc2b8ad537502d408bc33af05727453295d6622 && $platform == linux-x86_64 ]] || \
    fail "import-ci is limited to historical revision 9dc2b8ad537502d408bc33af05727453295d6622 on linux-x86_64; current submodule revision $revision must use build"
  ci_repo=zilliztech/knowhere
  ci_run=34990984807
  ci_artifact=10409177308
  ci_merge=02c5a95a0a9265772677f26dd78bc1d56eb8f3c1
  ci_tree=216cfda80e74a116687edeb0c6eac4981566c4b3
  gh api "repos/$ci_repo/actions/artifacts/$ci_artifact" > "$verification/artifact.json"
  jq -e --arg revision "$revision" --argjson run "$ci_run" '
    .name == "jni-linux-x86_64" and .expired == false and
    .workflow_run.id == $run and .workflow_run.head_sha == $revision and
    .digest == "sha256:c92511fe2b868a584cfd2e89d33515e941440726a6bdb3601387f2a5fc036b52"
  ' "$verification/artifact.json" >/dev/null || fail 'CI artifact provenance does not match the recorded build'
  for commit in "$revision" "$ci_merge"; do
    gh api "repos/$ci_repo/git/commits/$commit" > "$verification/commit-$commit.json"
    jq -e --arg tree "$ci_tree" '.tree.sha == $tree' "$verification/commit-$commit.json" >/dev/null || fail 'CI merge and pinned branch do not have the expected identical source tree'
  done
  if [[ -z $artifact_dir ]]; then
    gh api "repos/$ci_repo/actions/artifacts/$ci_artifact/zip" > "$verification/artifact.zip"
    verify_sha c92511fe2b868a584cfd2e89d33515e941440726a6bdb3601387f2a5fc036b52 "$verification/artifact.zip"
    artifact_dir=$verification/artifact
    mkdir "$artifact_dir"
    unzip -q "$verification/artifact.zip" -d "$artifact_dir"
  else
    artifact_dir=$(cd "$artifact_dir" && pwd)
  fi
  jars=$artifact_dir/java/target
  verify_sha ff7a5c927c6aa769b3d6391e14c9815509e94cc752947a53c1c5d3e47f9f4f99 "$jars/knowhere-jni-1.0.0-SNAPSHOT.jar"
  verify_sha 154c5bc40bb78f66e11d96ae969fa7e22269591e01d313817181c647958ce99e "$jars/knowhere-jni-1.0.0-SNAPSHOT-linux-x86_64.jar"
  [[ $(head -n 1 "$artifact_dir/build/verification/toolchain.txt") == "$ci_merge" ]] || fail 'Unexpected commit in CI toolchain provenance'
  cp -R "$artifact_dir/build/verification" "$verification/upstream"
  cp -R "$jars/surefire-reports" "$verification/surefire-reports"
  source_provenance="ci.repository=$ci_repo
ci.run=$ci_run
ci.artifact=$ci_artifact
ci.merge=$ci_merge
git.tree=$ci_tree"
else
  for tool in git conan cmake ctest mvn patchelf readelf; do require "$tool"; done
  [[ $(conan --version) == 'Conan version 2.'* ]] || fail 'Conan 2 is required'
  [[ $(cmake --version | head -n 1) == 'cmake version 3.'* ]] || fail 'Use CMake 3; older dependency recipes may fail with CMake 4'
  work=$root/target/knowhere-build/$revision$variant/$platform
  source_dir=$work/source
  mkdir -p "$work"
  if [[ ! -d $source_dir ]]; then
    git init -q "$source_dir"
    git -C "$source_dir" fetch --depth=1 "$submodule" "$revision"
    git -C "$source_dir" checkout -q --detach FETCH_HEAD
  fi
  [[ $(git -C "$source_dir" rev-parse HEAD) == "$revision" ]] || fail 'Build checkout does not match the pin'
  [[ -z $(git -C "$source_dir" status --porcelain --untracked-files=no) ]] || fail 'Build checkout has modified tracked files'
  cardinal_provenance=
  if [[ $with_cardinal == true ]]; then
    for generation in v1 v2; do
      cardinal_cmake=$source_dir/cmake/libs/cardinal/$generation/CMakeLists.txt
      cardinal_tag=$(sed -n 's/^set(CARDINAL_VERSION \([^)]*\)).*/\1/p' "$cardinal_cmake")
      [[ $cardinal_tag =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "Missing pinned Cardinal version in $cardinal_cmake"
      cardinal_source=$source_dir/thirdparty/cardinal$generation
      if [[ ! -d $cardinal_source/.git ]]; then
        git clone --no-checkout --no-hardlinks "$cardinal_repository" "$cardinal_source"
        git -C "$cardinal_source" checkout --detach "$cardinal_tag"
      fi
      cardinal_revision=$(git -C "$cardinal_source" rev-parse "$cardinal_tag^{commit}")
      [[ $(git -C "$cardinal_source" rev-parse HEAD) == "$cardinal_revision" ]] || fail "Cardinal $generation checkout differs from $cardinal_tag"
      [[ -z $(git -C "$cardinal_source" status --porcelain --untracked-files=no) ]] || fail "Cardinal $generation has modified tracked files"
      cardinal_provenance+="cardinal.$generation.tag=$cardinal_tag"$'\n'
      cardinal_provenance+="cardinal.$generation.revision=$cardinal_revision"$'\n'
    done
    printf '%s' "$cardinal_provenance" > "$verification/cardinal.properties"
  fi
  # Keep Conan profile and remote changes out of the developer's usual cache.
  export CONAN_HOME=${KNOWHERE_CONAN_HOME:-$work/conan-home}
  export CC=${CC:-gcc-12} CXX=${CXX:-g++-12} FC=${FC:-gfortran-12}
  require "$CC"; require "$CXX"; require "$FC"
  export OMP_NUM_THREADS=$jobs OPENBLAS_NUM_THREADS=$jobs
  conan profile detect --force
  cmake_version=$(cmake --version | head -n 1 | awk '{print $3}')
  profile=$work/jni-profile
  printf 'include(default)\n[platform_tool_requires]\ncmake/%s\n' "$cmake_version" > "$profile"
  conan remote add default-conan-local2 https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2 --force
  cd "$source_dir"
  options=(-pr:h "$profile" -pr:b "$profile" -s compiler.cppstd=20 -s:b compiler.cppstd=20 -s build_type=Release
    -o '&:with_c_api=True' -o '&:with_jni=True' -o '&:with_c_api_tests=True' -o '&:with_diskann=True')
  if [[ $with_cardinal == true ]]; then options+=(-o '&:with_cardinal=True'); fi
  conan install . -of build "${options[@]}" -c "tools.build:jobs=$jobs" --build=missing
  conan graph info . "${options[@]}" --no-remote --format=json > "$verification/conan-graph.json"
  { git rev-parse HEAD; "$CC" --version; ldd --version; java -version; conan --version; cmake --version; } > "$verification/toolchain.txt" 2>&1
  cmake_options=()
  if [[ $with_cardinal == true ]]; then cmake_options+=(-DWITH_CARDINAL:BOOL=ON); fi
  cmake -S . -B build/Release -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    "-DCMAKE_TOOLCHAIN_FILE=$source_dir/build/Release/generators/conan_toolchain.cmake" "${cmake_options[@]}"
  cp build/Release/CMakeCache.txt "$verification/CMakeCache.txt"
  cp build/Release/compile_commands.json "$verification/compile_commands.json"
  if [[ $with_cardinal == true ]]; then
    grep -Eq '^WITH_CARDINAL:BOOL=(ON|TRUE|1)$' "$verification/CMakeCache.txt" || fail 'CMake did not enable WITH_CARDINAL'
  fi
  cmake --build build/Release --parallel "$jobs"
  set +u
  source build/Release/generators/conanrun.sh
  set -u
  ctest --test-dir build/Release --output-on-failure | tee "$verification/ctest.log"
  LD_PRELOAD="$JAVA_HOME/lib/libjsig.so${LD_PRELOAD:+:$LD_PRELOAD}" mvn -B -f java/pom.xml test \
    -Dtest=KnowhereTest,DiskAnnIT -DargLine=-Xcheck:jni \
    "-Dknowhere.native.path=$source_dir/build/Release/java/libknowhere_jni.so" 2>&1 | tee "$verification/jni-maven.log"
  python3 java/scripts/check_jni_diagnostics.py "$verification/jni-maven.log" java/target/surefire-reports
  python3 -m unittest discover -s java/tests -v
  python3 java/scripts/bundle_native.py --library "$source_dir/build/Release/java/libknowhere_jni.so" \
    --output "$source_dir/java/target/native-resources" --platform "$platform"
  mvn -B -f java/pom.xml package "-Dnative.platform=$platform" -DskipTests
  jars=$source_dir/java/target
  cp -R "$jars/surefire-reports" "$verification/surefire-reports"
  source_provenance="git.repository=$repository
git.object.source=$submodule
build.source=$source_dir
build.jobs=$jobs
$cardinal_provenance"
fi

api_jar=$jars/knowhere-jni-1.0.0-SNAPSHOT.jar
native_jar=$jars/knowhere-jni-1.0.0-SNAPSHOT-$platform.jar
[[ -f $api_jar && -f $native_jar ]] || fail 'Upstream API/platform JARs were not produced'
for jar in "$api_jar" "$native_jar"; do
  destination=$output/$(basename "$jar")
  cp "$jar" "$destination"
  {
    printf 'git.revision=%s\njar.sha256=%s\n' "$revision" "$(sha256sum "$destination" | awk '{print $1}')"
    printf 'platform=%s\nverification.directory=%s\n' "$platform" "$verification"
    printf 'build.action=%s\n%s\n' "$action" "$source_provenance"
    printf 'build.with_cardinal=%s\n' "$with_cardinal"
  } > "$destination.properties"
done
unzip -p "$native_jar" "native/knowhere/1/$platform/manifest.properties" > "$verification/manifest.properties"
unzip -p "$native_jar" "native/knowhere/1/$platform/missing-licenses.txt" > "$verification/missing-licenses.txt"
if [[ -s $verification/missing-licenses.txt ]]; then
  printf '\nUpstream license material is incomplete; keep this package local:\n'
  cat "$verification/missing-licenses.txt"
fi
printf '\nNative platform JAR: %s\nProvenance: %s.properties\n' "$output/$(basename "$native_jar")" "$output/$(basename "$native_jar")"
