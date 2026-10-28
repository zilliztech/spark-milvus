# Connector image: package an existing Spark 4.0 assembly with the examples.
# Native libraries are already in the JAR; docker/build.sh stages the context.
ARG BASE_IMAGE=spark:4.0.1-scala2.13-java21-python3-ubuntu
FROM ${BASE_IMAGE}

USER root
ARG ASSEMBLY_SHA256
LABEL io.zilliz.spark-milvus.assembly-sha256="${ASSEMBLY_SHA256}"

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    CONNECTOR_JAR=/opt/spark-milvus/connector.jar \
    MILVUS_DEMO_ROOT=/opt/spark-milvus \
    PYTHONDONTWRITEBYTECODE=1

COPY connector.jar /opt/spark-milvus/connector.jar
COPY LICENSE /opt/spark-milvus/LICENSE
COPY demo/ /opt/spark-milvus/demo/
COPY scripts/ci/check_read_fixtures.py /opt/spark-milvus/scripts/ci/check_read_fixtures.py
COPY core/src/test/resources/read-fixtures/collections/nearest/ /opt/spark-milvus/core/src/test/resources/read-fixtures/collections/nearest/

# Fail on an incompatible base, verify the staged assembly and frozen sample,
# then compile the Java entry points once for direct spark-submit deployment.
RUN set -eu; \
    test -x /opt/entrypoint.sh; \
    test -x "$SPARK_HOME/bin/spark-submit"; \
    test -f "$JAVA_HOME/lib/libjsig.so"; \
    "$JAVA_HOME/bin/java" -version 2>&1 | grep -E 'version "21([.+-]|")'; \
    test -f "$SPARK_HOME/jars/spark-core_2.13-4.0.1.jar"; \
    printf '%s  %s\n' "$ASSEMBLY_SHA256" "$CONNECTOR_JAR" | sha256sum --check --strict -; \
    python3 -c 'from pathlib import Path; import runpy; check = runpy.run_path("/opt/spark-milvus/scripts/ci/check_read_fixtures.py"); check["check_group"](Path("/opt/spark-milvus/core/src/test/resources/read-fixtures/collections/nearest"))'; \
    mkdir /tmp/spark-milvus-demo-classes; \
    find /opt/spark-milvus/demo/java/src/main/java -type f -name '*.java' | sort > /tmp/spark-milvus-demo-sources; \
    "$JAVA_HOME/bin/javac" --release 17 -encoding UTF-8 \
        -classpath "$SPARK_HOME/jars/*:$CONNECTOR_JAR" \
        -d /tmp/spark-milvus-demo-classes @/tmp/spark-milvus-demo-sources; \
    "$JAVA_HOME/bin/jar" --create --file /opt/spark-milvus/demo-java.jar \
        -C /tmp/spark-milvus-demo-classes .; \
    rm -rf /tmp/spark-milvus-demo-classes /tmp/spark-milvus-demo-sources; \
    chmod 755 /opt/spark-milvus/demo/run.sh; \
    install -d -o 185 -g 185 -m 755 /opt/spark-milvus/demo/target /opt/spark/work-dir; \
    chmod 1777 /tmp

# Both driver and executor JVMs inherit signal chaining and allocator settings.
ENV LD_PRELOAD=/opt/java/openjdk/lib/libjsig.so \
    LD_BIND_NOW=1 \
    MALLOC_ARENA_MAX=2 \
    MALLOC_MMAP_THRESHOLD_=1048576 \
    MALLOC_TRIM_THRESHOLD_=134217728

# Spark downloads submitted JARs into its working directory. Keep that separate
# from the image's immutable source JARs so a driver cannot overwrite them.
WORKDIR /opt/spark/work-dir
USER 185:185

# Inherit /opt/entrypoint.sh: its driver/executor modes are required by Spark on
# Kubernetes, while other commands (including the local demos) pass through.
CMD ["/opt/spark-milvus/demo/run.sh", "--help"]
