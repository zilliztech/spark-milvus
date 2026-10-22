#!/bin/bash
# Transitional public copy; maintained caller: zilliztech/milvus-offline-compute/
# scripts/run_backfill_demo.sh. Removal belongs to T15; new callers use the private candidate.
#
# Snapshot backfill end to end, in the steps of
# docs/user-guide-snapshot-backfill.md: backfill_demo.py prepare adds two
# nullable fields to a new collection and takes a snapshot, BackfillApp fills
# them from a parquet file under spark-submit, the Proxy management endpoint
# commits the result, and backfill_demo.py verify queries the values.
#
# Needs a Milvus 3.0+ with its management endpoint and the object storage it
# writes to (the defaults are a local Milvus standalone with MinIO), SPARK_HOME
# pointing at Spark 4.0 for Scala 2.13, JAVA_HOME at Java 21, python3 (or
# PYTHON) with pymilvus 3.0+, numpy and pyarrow, and root's fat jar built with
# the native bundle (make package). spark-submit downloads hadoop-aws, through
# which BackfillApp writes the result JSON. Override any default below through
# the environment; KEEP=1 keeps the collection and its snapshot.
#
# Usage: ./scripts/run_backfill_demo.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

: "${SPARK_HOME:?Set SPARK_HOME to a Spark 4.0 distribution}"
: "${JAVA_HOME:?Set JAVA_HOME to a Java 21 JDK}"
JAR="${JAR:-$(ls -t "$PROJECT_DIR"/target/scala-2.13/spark-connector-assembly-*.jar 2>/dev/null | head -1)}"
if [ ! -f "$JAR" ]; then
  echo "No connector jar: build it with 'make package', or set JAR" >&2
  exit 1
fi

export MILVUS_URI="${MILVUS_URI:-http://localhost:19530}"
export MILVUS_TOKEN="${MILVUS_TOKEN:-root:Milvus}"
export S3_BUCKET="${S3_BUCKET:-a-bucket}"
MILVUS_MANAGEMENT="${MILVUS_MANAGEMENT:-http://localhost:9091}"
S3_ENDPOINT="${S3_ENDPOINT:-localhost:9000}"
S3_ROOT_PATH="${S3_ROOT_PATH:-files}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_REGION="${S3_REGION:-us-east-1}"
PYTHON="${PYTHON:-python3}"
SSL=()
[ "${S3_USE_SSL:-false}" = true ] && SSL=(--s3-use-ssl)

# The JVM's signal-chaining library is preloaded into the driver here and into
# the executors through spark.executorEnv.
if [ "$(uname)" = Darwin ]; then
  PRELOAD=DYLD_INSERT_LIBRARIES JSIG="$JAVA_HOME/lib/libjsig.dylib"
else
  PRELOAD=LD_PRELOAD JSIG="$JAVA_HOME/lib/libjsig.so"
fi

WORK="$(mktemp -d)"
field() { "$PYTHON" -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$WORK/prepare.json" "$1"; }

echo "== 1-2. Add the fields and take a snapshot"
"$PYTHON" "$SCRIPT_DIR/backfill_demo.py" prepare "$WORK" > "$WORK/prepare.json"
COLLECTION="$(field collection)"
cleanup() { [ "${KEEP:-0}" = 1 ] || "$PYTHON" "$SCRIPT_DIR/backfill_demo.py" drop "$COLLECTION"; }
trap cleanup EXIT

echo "== 3. Run BackfillApp"
RESULT="s3a://$S3_BUCKET/backfill/$COLLECTION.json"
HADOOP_VERSION="$(ls "$SPARK_HOME"/jars/hadoop-client-api-*.jar | sed -E 's/.*hadoop-client-api-(.+)\.jar/\1/')"
env "$PRELOAD=$JSIG" "$SPARK_HOME/bin/spark-submit" \
  --master "${MASTER:-local[*]}" \
  --packages "org.apache.hadoop:hadoop-aws:$HADOOP_VERSION" \
  --exclude-packages software.amazon.awssdk:bundle \
  --conf spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin \
  --conf 'spark.redaction.regex=(?i)secret|password|token|access[._]?key' \
  --conf "spark.executorEnv.$PRELOAD=$JSIG" \
  --class com.zilliz.spark.connector.apps.backfill.BackfillApp \
  "$JAR" \
  --snapshot "$(field snapshot)" \
  --parquet "$(field parquet)" \
  --s3-endpoint "$S3_ENDPOINT" \
  --s3-bucket "$S3_BUCKET" \
  --s3-root-path "$S3_ROOT_PATH" \
  --s3-region "$S3_REGION" \
  --s3-access-key "$S3_ACCESS_KEY" \
  --s3-secret-key "$S3_SECRET_KEY" \
  ${SSL[@]+"${SSL[@]}"} \
  --mode replace \
  --output-result "$RESULT"

echo "== 4. Commit $RESULT"
curl --fail-with-body --silent --show-error --get \
  "$MILVUS_MANAGEMENT/management/datacoord/backfill/commit" \
  --data-urlencode "result_path=$RESULT" | tee "$WORK/commit.json"
echo
"$PYTHON" -c 'import json, sys
answer = json.load(open(sys.argv[1]))
failed = answer.get("failed_segments")
print("committed %s of %s segments, %s failed"
      % (answer.get("committed_segments"), answer.get("total_segments"), failed))
sys.exit(0 if failed == 0 else 1)' "$WORK/commit.json"

echo "== 5. Query the new fields"
"$PYTHON" "$SCRIPT_DIR/backfill_demo.py" verify "$COLLECTION"
