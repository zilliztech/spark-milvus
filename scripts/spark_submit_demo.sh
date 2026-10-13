#!/bin/bash
#
# Submits scripts/search_demo.py with spark-submit, with the settings an
# application that reads and searches Milvus needs: the connector's jar, its
# session extension and plugin, MilvusCatalog and the signal-chaining preload
# the native libraries ask for. The program creates a small collection, reads it
# through the catalog and searches it with NEAREST BY.
#
# Needs a Milvus 3.0+ and the object storage it writes to (the defaults are a
# local Milvus standalone with MinIO), SPARK_HOME pointing at Spark 4.0 for
# Scala 2.13, JAVA_HOME at Java 21, a Python with pymilvus 3.0+ and numpy for
# spark-submit to run (python3, or PYSPARK_PYTHON), and root's fat jar built
# with the native bundle (make package). Override any default below through the
# environment.
#
# Usage: ./scripts/spark_submit_demo.sh

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
S3_ENDPOINT="${S3_ENDPOINT:-localhost:9000}"
S3_BUCKET="${S3_BUCKET:-a-bucket}"
S3_ROOT_PATH="${S3_ROOT_PATH:-files}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_USE_SSL="${S3_USE_SSL:-false}"

# The JVM's signal-chaining library is preloaded into the driver here and into
# the executors through spark.executorEnv.
if [ "$(uname)" = Darwin ]; then
  PRELOAD=DYLD_INSERT_LIBRARIES JSIG="$JAVA_HOME/lib/libjsig.dylib"
else
  PRELOAD=LD_PRELOAD JSIG="$JAVA_HOME/lib/libjsig.so"
fi
export "$PRELOAD=$JSIG"

CATALOG=spark.sql.catalog.milvus
"$SPARK_HOME/bin/spark-submit" \
  --master "${MASTER:-local[*]}" \
  --jars "$JAR" \
  --conf spark.sql.extensions=com.zilliz.spark.connector.extensions.MilvusSparkSessionExtensions \
  --conf spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin \
  --conf "spark.executorEnv.$PRELOAD=$JSIG" \
  --conf "$CATALOG=com.zilliz.spark.connector.catalog.MilvusCatalog" \
  --conf "$CATALOG.milvus.uri=$MILVUS_URI" \
  --conf "$CATALOG.milvus.token=$MILVUS_TOKEN" \
  --conf "$CATALOG.fs.address=$S3_ENDPOINT" \
  --conf "$CATALOG.fs.bucket_name=$S3_BUCKET" \
  --conf "$CATALOG.fs.root_path=$S3_ROOT_PATH" \
  --conf "$CATALOG.fs.access_key_id=$S3_ACCESS_KEY" \
  --conf "$CATALOG.fs.access_key_value=$S3_SECRET_KEY" \
  --conf "$CATALOG.fs.region=$S3_REGION" \
  --conf "$CATALOG.fs.use_ssl=$S3_USE_SSL" \
  --conf "$CATALOG.fs.use_virtual_host=false" \
  "$SCRIPT_DIR/search_demo.py"
