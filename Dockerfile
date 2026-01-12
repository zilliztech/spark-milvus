# syntax=docker/dockerfile:1.4
# Build spark-milvus connector with milvus-storage native libraries

# Build arguments
ARG GIT_BRANCH=unknown

# Stage 1: Build milvus-storage native libraries and Java binding
FROM spark:4.0.1-scala2.13-java21-python3-ubuntu AS builder

ARG GIT_BRANCH

USER root

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Install dependencies for building milvus-storage
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget curl git g++ gcc make ccache gdb \
    python3 python3-pip \
    zip unzip \
    automake autoconf libtool \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aclocal-1.16 /usr/bin/aclocal-1.15 \
    && ln -sf /usr/bin/automake-1.16 /usr/bin/automake-1.15

# Install CMake (architecture-aware, same pattern as milvus-storage)
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$(uname -m).tar.gz" | tar --strip-components=1 -xz -C /usr/local

# Install Conan
RUN pip3 install conan==1.61.0

# Setup Conan profile and remote (handle existing profile from cache)
RUN conan profile new default --detect || true \
    && conan profile update settings.compiler.libcxx=libstdc++11 default
RUN conan remote add default-conan-local https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local --insert || true

# Set ccache configuration
ENV CCACHE_DIR=/root/.ccache
ENV PATH=/usr/lib/ccache:$PATH

# Use Java 21 from base image, install Scala/sbt via SDKMAN
ENV SDKMAN_DIR=/root/.sdkman
RUN curl -s "https://get.sdkman.io" | bash
RUN bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && \
    sdk install scala 2.13.16 && \
    sdk install sbt 1.11.1"

# JAVA_HOME is already set in base image (java21)
ENV SCALA_HOME=/root/.sdkman/candidates/scala/current
ENV SBT_HOME=/root/.sdkman/candidates/sbt/current
ENV PATH=$SCALA_HOME/bin:$SBT_HOME/bin:$PATH

WORKDIR /workspace

COPY . .

# Initialize git submodules
RUN git config --global --add safe.directory /workspace && \
    git config --global --add safe.directory /workspace/milvus-proto && \
    git config --global --add safe.directory /workspace/milvus-storage && \
    git submodule update --init --recursive

# Build milvus-storage native library using its Makefile (with cache, no ASAN)
# Ensure conan profile and remote are set up before build (in case cache is empty)
RUN --mount=type=cache,target=/root/.conan \
    --mount=type=cache,target=/root/.ccache \
    conan profile new default --detect 2>/dev/null || true && \
    conan profile update settings.compiler.libcxx=libstdc++11 default && \
    conan remote add default-conan-local https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local --insert 2>/dev/null || true && \
    cd milvus-storage/cpp && make build USE_JNI=True WITH_UT=False USE_ASAN=False

# Copy native libs
RUN mkdir -p milvus-storage/java/native src/main/resources/native && \
    cp milvus-storage/cpp/build/Release/libmilvus-storage.so milvus-storage/java/native/ && \
    cp milvus-storage/cpp/build/Release/libmilvus-storage-jni.so milvus-storage/java/native/ && \
    cp milvus-storage/cpp/build/Release/libmilvus-storage.so src/main/resources/native/ && \
    cp milvus-storage/cpp/build/Release/libmilvus-storage-jni.so src/main/resources/native/

# Build milvus-storage Java binding (with cache, persist local publish)
RUN --mount=type=cache,target=/root/.ivy2 \
    --mount=type=cache,target=/root/.sbt \
    cd milvus-storage/java && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sbt publishLocal" && \
    mkdir -p /workspace/.ivy2-local && cp -r /root/.ivy2/local/* /workspace/.ivy2-local/

# Build spark-milvus connector (with cache)
ENV GIT_BRANCH=${GIT_BRANCH}
ENV SBT_OPTS="-Xmx4g -Xms2g"
RUN --mount=type=cache,target=/root/.ivy2 \
    --mount=type=cache,target=/root/.sbt \
    bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sbt 'compile; Test/compile; package'"

# Stage 2: Final image (only copy artifacts and publish)
FROM spark:4.0.1-scala2.13-java21-python3-ubuntu AS final

ARG GIT_BRANCH
ENV GIT_BRANCH=${GIT_BRANCH}

USER root

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl zip unzip \
    && rm -rf /var/lib/apt/lists/*

# Use Java 21 from base image, install Scala/sbt via SDKMAN
ENV SDKMAN_DIR=/root/.sdkman
RUN curl -s "https://get.sdkman.io" | bash
RUN bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && \
    sdk install scala 2.13.16 && \
    sdk install sbt 1.11.1"

# JAVA_HOME is already set in base image (java21)
ENV SCALA_HOME=/root/.sdkman/candidates/scala/current
ENV SBT_HOME=/root/.sdkman/candidates/sbt/current
ENV PATH=$SCALA_HOME/bin:$SBT_HOME/bin:$PATH

WORKDIR /workspace

# Copy only necessary files for sbt publish (not entire workspace)
COPY --from=builder /workspace/build.sbt /workspace/build.sbt
COPY --from=builder /workspace/project/ /workspace/project/
COPY --from=builder /workspace/target/ /workspace/target/
COPY --from=builder /workspace/.ivy2-local /root/.ivy2/local

ARG PUBLISH_TO_CENTRAL=true
ENV SBT_OPTS="-Xmx4g -Xms2g"
RUN --mount=type=cache,target=/root/.sbt \
    if [ "$PUBLISH_TO_CENTRAL" = "true" ]; then \
        bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sbt publish"; \
    fi

CMD ["/bin/bash"]
