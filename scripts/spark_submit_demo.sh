#!/bin/bash
# Read and search a fixed V3 snapshot with exact candidate JVM/native bytes.
# Usage: CANDIDATE=/verified/public-candidate ./scripts/spark_submit_demo.sh \
#          --options /private/read-options.json --queries /data/queries.json
# No Milvus service, SDK, Catalog, root assembly lookup or package download.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${SPARK_HOME:?Set SPARK_HOME to Spark 4.0.1 for Scala 2.13}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 21 JDK}"
: "${CANDIDATE:?Set CANDIDATE to a verified public release candidate directory}"
PYTHON="${PYTHON:-python3}"
JARS="$($PYTHON - "$SCRIPT_DIR" "$CANDIDATE" <<'PY'
import platform, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import release
root = Path(sys.argv[2]).resolve(strict=True)
manifest = release.verify(root)
machine = {'x86_64': 'x86_64', 'aarch64': 'aarch64', 'arm64': 'aarch64'}.get(platform.machine())
if platform.system() != 'Linux' or machine is None:
    raise ValueError('The candidate example requires a supported Linux platform')
selected = []
for kind in ('jvm', 'native'):
    matches = [entry for entry in manifest['artifacts'] if entry['kind'] == kind
               and not entry['classifier'] and entry['extension'] == 'jar'
               and (entry['line'] == '4.0' if kind == 'jvm' else entry['platform'] == 'linux-' + machine)]
    if len(matches) != 1:
        raise ValueError('Candidate must supply exactly one Spark 4.0 JVM and matching OSS native JAR')
    path = str(root / 'repository' / matches[0]['path'])
    if any(char in path for char in (',', '\n', '\r')):
        raise ValueError('Candidate path cannot contain commas or newlines')
    selected.append(path)
print(','.join(selected))
PY
)"
JSIG="$JAVA_HOME/lib/libjsig.so"
test -f "$JSIG" || { echo "Missing Java signal-chaining library" >&2; exit 1; }
export LD_PRELOAD="$JSIG${LD_PRELOAD:+ $LD_PRELOAD}"
export LD_BIND_NOW=1
exec "$SPARK_HOME/bin/spark-submit" \
  --master "${MASTER:-local[2]}" --jars "$JARS" \
  --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusReadSparkSessionExtensions \
  --conf spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin \
  --conf 'spark.redaction.regex=(?i)secret|password|token|access[._]?key' \
  --conf "spark.executorEnv.LD_PRELOAD=$JSIG" \
  --conf spark.executorEnv.LD_BIND_NOW=1 \
  "$SCRIPT_DIR/search_demo.py" "$@"
