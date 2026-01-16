#!/bin/bash
#
# Run Milvus Backfill Demo with PySpark
#
# Usage:
#   ./scripts/run_backfill_demo.sh
#

set -e

echo "========================================"
echo "Milvus Backfill Demo Runner"
echo "========================================"
echo ""

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

# Check if JAR exists
JAR_FILE="$PROJECT_DIR/target/scala-2.13/spark-connector-assembly-snapshot_reader-amd64-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "✗ JAR file not found: $JAR_FILE"
    echo ""
    echo "Please build the project first:"
    echo "  cd $PROJECT_DIR"
    echo "  sbt assembly"
    exit 1
fi

echo "✓ Found JAR: $JAR_FILE"
echo ""

# Check Python dependencies
echo "Checking Python dependencies..."
python3 -c "import pyspark; import pymilvus; import numpy" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "✗ Missing Python dependencies"
    echo ""
    echo "Please install required packages:"
    echo "  pip install pyspark pymilvus numpy"
    exit 1
fi

echo "✓ Python dependencies OK"
echo ""

# Check Milvus connection
echo "Checking Milvus connection..."
python3 -c "
from pymilvus import connections
try:
    connections.connect(uri='http://localhost:19530', token='root:Milvus')
    print('✓ Milvus connection OK')
    connections.disconnect('default')
except Exception as e:
    print('✗ Cannot connect to Milvus:', e)
    print('')
    print('Please ensure Milvus is running at localhost:19530')
    exit(1)
"

if [ $? -ne 0 ]; then
    exit 1
fi

echo ""
echo "========================================"
echo "Starting demo..."
echo "========================================"
echo ""

# Use Scala 2.13 version of Spark
SPARK_213_HOME="/home/shaoting/spark/spark-3.5.3-bin-hadoop3-scala2.13"
SPARK_SUBMIT="$SPARK_213_HOME/bin/spark-submit"

# Set environment to use Scala 2.13 Spark for everything
export SPARK_HOME="$SPARK_213_HOME"
export PATH="$SPARK_213_HOME/bin:$PATH"
export PYTHONPATH="$SPARK_213_HOME/python:$SPARK_213_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"

echo "Using Spark with Scala 2.13: $SPARK_213_HOME"
echo "SPARK_HOME: $SPARK_HOME"
echo "PYTHONPATH configured for Scala 2.13"
echo ""

# Native library paths - DO NOT set LD_PRELOAD in shell env, only pass to Spark
NATIVE_LIB="$PROJECT_DIR/src/main/resources/native/libmilvus-storage.so"
NATIVE_LIB_DIR="$PROJECT_DIR/src/main/resources/native"

# Set LD_LIBRARY_PATH for the driver (but NOT LD_PRELOAD which causes StackOverflow)
export LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:$NATIVE_LIB_DIR:$LD_LIBRARY_PATH"

echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "NATIVE_LIB: $NATIVE_LIB"
echo ""

# Run with spark-submit
LOG4J2_CONFIG="$PROJECT_DIR/src/test/resources/log4j2.properties"

"$SPARK_SUBMIT" \
  --master local[*] \
  --jars "$JAR_FILE" \
  --packages "org.apache.hadoop:hadoop-aws:3.3.4" \
  --files "$LOG4J2_CONFIG" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.configurationFile=file:$LOG4J2_CONFIG -Djava.library.path=$NATIVE_LIB_DIR" \
  --conf "spark.driver.extraLibraryPath=$NATIVE_LIB_DIR" \
  --conf "spark.executor.extraLibraryPath=$NATIVE_LIB_DIR" \
  --conf "spark.executorEnv.LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu:/lib/x86_64-linux-gnu:$NATIVE_LIB_DIR" \
  --conf "spark.driver.userClassPathFirst=true" \
  --conf "spark.executor.userClassPathFirst=true" \
  "$SCRIPT_DIR/backfill_demo.py"

exit_code=$?

echo ""
if [ $exit_code -eq 0 ]; then
    echo "========================================"
    echo "✓ Demo completed successfully!"
    echo "========================================"
else
    echo "========================================"
    echo "✗ Demo failed with exit code: $exit_code"
    echo "========================================"
fi

exit $exit_code
