#!/bin/bash

# Spark Submit Demo Script
# This script demonstrates how to submit a Spark application using spark-submit

set -e

export LD_PRELOAD=$(pwd)/src/main/resources/native/libmilvus-storage.so && \
    spark-submit \
    --master "local[*]" \
    --conf "spark.driver.extraJavaOptions=-Xss2m
    -Djava.library.path=$(pwd)/src/main/resources/native
    --add-opens=java.base/java.nio=ALL-UNNAMED" \
    --conf "spark.driver.userClassPathFirst=true" \
    --conf "spark.executor.userClassPathFirst=true" \
    --class example.MilvusStorageMain \
    target/scala-2.13/spark-connector-assembly-0.2.1-SNAPSHOT.jar