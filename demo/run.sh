#!/bin/bash
# Run one public connector example with an explicit assembly or release candidate.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON="${PYTHON:-python3}"
if [[ $# == 0 || "$1" == --help || "$1" == -h ]]; then
  cat <<'HELP'
Usage: ./demo/run.sh python <example-name-or-file> [example arguments]
       ./demo/run.sh java <class-name> [example arguments]

Run one example on Linux with Spark 4.0 / Scala 2.13 and Java 21:
  Python                    Java                Feature
  00_snapshot_path.py        SnapshotPath        Look up an existing snapshot path
  01_read_snapshot.py        ReadSnapshot        Read snapshot schema and rows
  02_filter_project.py       FilterProject       Filter rows and select columns
  03_sql_analytics.py        SqlAnalytics        Query with Spark SQL
  04_exact_search.py         ExactSearch         Find exact nearest neighbors
  05_filtered_search.py      FilteredSearch      Filter before vector search
  06_index_search.py         IndexSearch         Search persisted indexes
  07_vector_functions.py     VectorFunctions     Compare vector scores
  08_partition_selection.py  PartitionSelection  Select snapshot partitions

The Python .py suffix is optional. For example-specific arguments:
  ./demo/run.sh python 04_exact_search.py --help
  ./demo/run.sh java ExactSearch --help

Set SPARK_HOME and JAVA_HOME, then choose exactly one connector source:
  CONNECTOR_JAR=/absolute/path/to/spark-milvus-4.0-bundle-<version>.jar
    A local spark/assembly JAR containing this platform's native libraries.
  CANDIDATE=/absolute/path/to/verified-public-candidate
    A verified release candidate with Spark 4.0 JVM and Linux native JARs.

The snapshot-path helpers call Milvus REST directly: Python needs only Python;
Java needs JAVA_HOME and SPARK_HOME/jars for Jackson. Neither needs a connector
JAR or starts Spark. Use MILVUS_URI and MILVUS_TOKEN for the API identity.

Optional environment: MASTER=local[2], PYTHON=python3.
Snapshot examples use the committed local snapshot by default. For a cluster,
provide --options with shared storage settings. VectorFunctions and Python
example 07 use in-memory vectors. The launcher uses Spark client deploy mode.
Java sources compile into an owned temporary directory under demo/target;
that directory is removed when the command exits. Dependencies are not downloaded.
HELP
  exit 0
fi
LANGUAGE="$1"
shift
case "$LANGUAGE" in
  python|java) ;;
  *) echo "Choose the language explicitly: python or java. Run ./demo/run.sh --help." >&2; exit 1 ;;
esac
if [[ $# == 0 ]]; then
  echo "Choose a $LANGUAGE example. Run ./demo/run.sh --help for the available examples." >&2
  exit 1
fi
EXAMPLE="$1"
shift
if [[ "$LANGUAGE" == python ]]; then
  EXAMPLE="${EXAMPLE%.py}.py"
  case "$EXAMPLE" in
    00_snapshot_path.py|01_read_snapshot.py|02_filter_project.py|03_sql_analytics.py|04_exact_search.py|\
    05_filtered_search.py|06_index_search.py|07_vector_functions.py|08_partition_selection.py) ;;
    *) echo "Unknown Python example: $EXAMPLE. Run ./demo/run.sh --help." >&2; exit 1 ;;
  esac
  test -f "$SCRIPT_DIR/python/$EXAMPLE" || {
    echo "Missing example: $SCRIPT_DIR/python/$EXAMPLE" >&2
    exit 1
  }
else
  case "$EXAMPLE" in
    SnapshotPath|ReadSnapshot|FilterProject|SqlAnalytics|ExactSearch|FilteredSearch|IndexSearch|VectorFunctions|PartitionSelection) ;;
    *) echo "Unknown Java example: $EXAMPLE. Run ./demo/run.sh --help." >&2; exit 1 ;;
  esac
fi
for argument in "$@"; do
  if [[ "$argument" == --help || "$argument" == -h ]]; then
    if [[ "$LANGUAGE" == python ]]; then
      exec "$PYTHON" "$SCRIPT_DIR/python/$EXAMPLE" --help
    fi
    echo "Usage: ./demo/run.sh java $EXAMPLE [options]"
    if [[ "$EXAMPLE" == SnapshotPath ]]; then
      cat <<'HELP'
  --uri URL              Milvus endpoint (or MILVUS_URI)
  --database NAME        Database name (default: default)
  --collection NAME      Collection containing the existing snapshot
  --snapshot NAME        Existing snapshot name
  --timeout SECONDS      HTTP request timeout
  --storage-options FILE Storage options JSON to combine with the snapshot path
  --output FILE          Write connector read options JSON
Authentication uses MILVUS_TOKEN. This helper does not create a snapshot.
HELP
      exit 0
    fi
    echo "  --explain          Print the Spark execution plan"
    if [[ "$EXAMPLE" != VectorFunctions ]]; then
      echo "  --options FILE     Snapshot options JSON; default: committed sample"
      echo "  --max-rows N       Driver verification limit (default/max: 10000)"
    fi
    case "$EXAMPLE" in
      FilterProject|ExactSearch|FilteredSearch|IndexSearch)
        echo "  --where SQL        Predicate applied before processing" ;;
    esac
    case "$EXAMPLE" in
      ExactSearch|FilteredSearch|IndexSearch)
        echo "  --queries FILE     Query vectors JSON; required with --options"
        echo "  --id-column NAME   Primary-key column (default: id)"
        echo "  --vector-column NAME  Vector column (default: vector)"
        echo "  --k N              Neighbors per query (default: 3)" ;;
    esac
    exit 0
  fi
done
if [[ "$LANGUAGE" == python && "$EXAMPLE" == 00_snapshot_path.py ]]; then
  exec "$PYTHON" "$SCRIPT_DIR/python/$EXAMPLE" "$@"
fi
: "${SPARK_HOME:?Set SPARK_HOME to Spark 4.0.1 for Scala 2.13}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 21 JDK}"
test -x "$JAVA_HOME/bin/java" || {
  echo "JAVA_HOME/bin/java must be executable: $JAVA_HOME/bin/java" >&2
  exit 1
}

prepare_java_build() {
  test -x "$JAVA_HOME/bin/javac" || { echo "JAVA_HOME/bin/javac must be executable" >&2; exit 1; }
  test -d "$SPARK_HOME/jars" || { echo "SPARK_HOME/jars must contain the Spark distribution's JARs" >&2; exit 1; }
  if [[ "$SPARK_HOME" == *:* || "$SCRIPT_DIR" == *:* ]]; then
    echo "Java classpath paths cannot contain colons" >&2
    exit 1
  fi
  mkdir -p "$SCRIPT_DIR/target"
  BUILD_DIR="$(mktemp -d "$SCRIPT_DIR/target/java-XXXXXXXX")"
  trap 'rm -rf -- "$BUILD_DIR"' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  mkdir "$BUILD_DIR/classes"
}

if [[ "$LANGUAGE" == java && "$EXAMPLE" == SnapshotPath ]]; then
  JAVA_VERSION="$("$JAVA_HOME/bin/java" -version 2>&1)"
  JAVA21_PATTERN='version "21([.+-]|")'
  if [[ ! "$JAVA_VERSION" =~ $JAVA21_PATTERN ]]; then
    echo "JAVA_HOME must select a Java 21 JDK" >&2
    exit 1
  fi
  SNAPSHOT_SOURCE="$SCRIPT_DIR/java/src/main/java/com/zilliz/spark/connector/demo/SnapshotPath.java"
  test -f "$SNAPSHOT_SOURCE" || { echo "Missing snapshot helper source: $SNAPSHOT_SOURCE" >&2; exit 1; }
  prepare_java_build
  "$JAVA_HOME/bin/javac" --release 17 -encoding UTF-8 \
    -classpath "$SPARK_HOME/jars/*" -d "$BUILD_DIR/classes" "$SNAPSHOT_SOURCE"
  STATUS=0
  "$JAVA_HOME/bin/java" -cp "$BUILD_DIR/classes:$SPARK_HOME/jars/*" \
    com.zilliz.spark.connector.demo.SnapshotPath "$@" || STATUS=$?
  exit "$STATUS"
fi

if [[ -n "${CANDIDATE:-}" && -n "${CONNECTOR_JAR:-}" ]] ||
   [[ -z "${CANDIDATE:-}" && -z "${CONNECTOR_JAR:-}" ]]; then
  echo "Set exactly one of CONNECTOR_JAR or CANDIDATE" >&2
  exit 1
fi
test -x "$SPARK_HOME/bin/spark-submit" || {
  echo "SPARK_HOME/bin/spark-submit must be executable: $SPARK_HOME/bin/spark-submit" >&2
  exit 1
}
MASTER="${MASTER:-local[2]}"
CUSTOM_INPUT=false
for argument in "$@"; do
  if [[ "$argument" == --options || "$argument" == --options=* ]]; then
    CUSTOM_INPUT=true
  fi
done
if [[ "$EXAMPLE" != 07_vector_functions.py && "$EXAMPLE" != VectorFunctions && "$CUSTOM_INPUT" == false &&
      ! "$MASTER" =~ ^local(\[([1-9][0-9]*|\*)(,[1-9][0-9]*)?\])?$ ]]; then
  echo "The default snapshot requires MASTER=local or local[...]; use --options with shared storage for a cluster" >&2
  exit 1
fi
SYSTEM="$(uname -s)"
ARCHITECTURE="$(uname -m)"
JARS="$("$PYTHON" - "$REPO_DIR/scripts" "${CANDIDATE:-}" "${CONNECTOR_JAR:-}" \
  "$SYSTEM" "$ARCHITECTURE" "$JAVA_HOME" "$LANGUAGE" <<'PY'
import re
import subprocess
import sys
from pathlib import Path
from zipfile import BadZipFile, ZipFile


def checked_path(value):
    path = Path(value).resolve(strict=True)
    if any(char in str(path) for char in (',', '\n', '\r')):
        raise ValueError('JAR and candidate paths cannot contain commas or newlines')
    if language == 'java' and ':' in str(path):
        raise ValueError('Java classpath paths cannot contain colons')
    return path


try:
    script_dir, candidate, assembly, system, architecture, java_home, language = sys.argv[1:]
    machine = {'x86_64': 'x86_64', 'aarch64': 'aarch64', 'arm64': 'aarch64'}.get(architecture)
    if system != 'Linux' or machine is None:
        raise ValueError('The demo supports Linux on x86_64 or aarch64')
    java = subprocess.run([str(Path(java_home) / 'bin/java'), '-version'],
                          text=True, capture_output=True, check=True)
    if not re.search(r'\bversion "21(?:[.+-]|")', java.stderr + java.stdout):
        raise ValueError('JAVA_HOME must select a Java 21 JDK')
    if assembly:
        path = checked_path(assembly)
        if not path.is_file():
            raise ValueError('CONNECTOR_JAR must name a regular JAR file')
        platform = 'linux-' + machine
        with ZipFile(path) as archive:
            required = {'com/zilliz/spark/connector/extensions/MilvusSparkPlugin.class',
                        f'native/milvus/1/{platform}/manifest.properties'}
            if not required.issubset(archive.namelist()):
                raise ValueError('CONNECTOR_JAR must be a spark/assembly JAR with native libraries for ' + platform)
        selected = [str(path)]
    else:
        sys.path.insert(0, script_dir)
        import release
        root = checked_path(candidate)
        manifest = release.verify(root)
        selected = []
        for kind in ('jvm', 'native'):
            matches = [entry for entry in manifest['artifacts'] if entry['kind'] == kind
                       and not entry['classifier'] and entry['extension'] == 'jar'
                       and (entry['line'] == '4.0' if kind == 'jvm' else entry['platform'] == 'linux-' + machine)]
            if len(matches) != 1:
                raise ValueError('Candidate must supply exactly one Spark 4.0 JVM and matching OSS native JAR')
            selected.append(str(checked_path(root / 'repository' / matches[0]['path'])))
    print(','.join(selected))
except (OSError, ValueError, BadZipFile, subprocess.CalledProcessError) as error:
    sys.exit('Demo setup failed: ' + str(error))
PY
)"
JSIG="$JAVA_HOME/lib/libjsig.so"
test -f "$JSIG" || { echo "Missing Java signal-chaining library" >&2; exit 1; }
SPARK_ARGUMENTS=(
  --master "$MASTER" --jars "$JARS" --deploy-mode client
  --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions
  --conf spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin
  --conf 'spark.redaction.regex=(?i)secret|password|token|access[._]?key'
  --conf "spark.executorEnv.LD_PRELOAD=$JSIG"
  --conf spark.executorEnv.LD_BIND_NOW=1
)
if [[ "$LANGUAGE" == python ]]; then
  export LD_PRELOAD="$JSIG${LD_PRELOAD:+:$LD_PRELOAD}"
  export LD_BIND_NOW=1
  exec "$SPARK_HOME/bin/spark-submit" "${SPARK_ARGUMENTS[@]}" \
    --py-files "$SCRIPT_DIR/python/common.py" "$SCRIPT_DIR/python/$EXAMPLE" "$@"
fi

test -x "$JAVA_HOME/bin/jar" || { echo "JAVA_HOME/bin/jar must be executable" >&2; exit 1; }
JAVA_SOURCE_ROOT="$SCRIPT_DIR/java/src/main/java"
JAVA_SOURCES=()
while IFS= read -r -d '' source; do
  JAVA_SOURCES+=("$source")
done < <(find "$JAVA_SOURCE_ROOT" -type f -name '*.java' -print0)
if [[ ${#JAVA_SOURCES[@]} == 0 ]]; then
  echo "No Java example sources found under $JAVA_SOURCE_ROOT" >&2
  exit 1
fi
prepare_java_build
"$JAVA_HOME/bin/javac" --release 17 -encoding UTF-8 \
  -classpath "$SPARK_HOME/jars/*:${JARS//,/:}" -d "$BUILD_DIR/classes" "${JAVA_SOURCES[@]}"
"$JAVA_HOME/bin/jar" --create --file "$BUILD_DIR/app.jar" -C "$BUILD_DIR/classes" .
export MILVUS_DEMO_ROOT="$REPO_DIR"
export LD_PRELOAD="$JSIG${LD_PRELOAD:+:$LD_PRELOAD}"
export LD_BIND_NOW=1
STATUS=0
"$SPARK_HOME/bin/spark-submit" "${SPARK_ARGUMENTS[@]}" \
  --class "com.zilliz.spark.connector.demo.$EXAMPLE" "$BUILD_DIR/app.jar" "$@" || STATUS=$?
exit "$STATUS"
