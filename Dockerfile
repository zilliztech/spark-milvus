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

# Install dependencies for building and packaging milvus-storage
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget curl git g++ gcc make ccache gdb \
    python3 python3-pip \
    zip unzip \
    automake autoconf libtool patchelf \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aclocal-1.16 /usr/bin/aclocal-1.15 \
    && ln -sf /usr/bin/automake-1.16 /usr/bin/automake-1.15

# Install CMake (architecture-aware, same pattern as milvus-storage)
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$(uname -m).tar.gz" | tar --strip-components=1 -xz -C /usr/local

# The pinned milvus-storage submodule requires Conan 2.
ENV CONAN_HOME=/root/.conan/conan2
RUN pip3 install --no-cache-dir conan==2.25.1

# Setup the Conan 2 profile and artifact remote used by milvus-storage.
RUN conan profile detect --force \
    && conan remote add --force default-conan-local2 \
        https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2

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

# Build milvus-storage native libraries using its Conan 2 Makefile.
RUN cd milvus-storage/cpp && make java-lib

# Package the JNI libraries and every transitive shared library under the
# platform path expected by NativeLibraryLoader.
RUN set -eux; \
    case "$(uname -m)" in \
        x86_64|amd64) native_platform=linux-x86_64 ;; \
        aarch64|arm64) native_platform=linux-aarch64 ;; \
        *) echo "Unsupported build architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    native_dir="src/main/resources/native/${native_platform}"; \
    libs_dir="milvus-storage/cpp/build/Release/libs"; \
    mkdir -p "${native_dir}"; \
    cp milvus-storage/cpp/build/Release/libmilvus-storage.so "${native_dir}/"; \
    cp milvus-storage/cpp/build/Release/libmilvus-storage-jni.so "${native_dir}/"; \
    if [ -d "${libs_dir}" ]; then \
        find -L "${libs_dir}" -maxdepth 1 -type f \
            \( -name '*.so' -o -name '*.so.*' \) \
            -exec cp -L {} "${native_dir}/" \;; \
        for subdir in ossl-modules engines-3; do \
            if [ -d "${libs_dir}/${subdir}" ]; then \
                mkdir -p "${native_dir}/${subdir}"; \
                cp -rL "${libs_dir}/${subdir}/." "${native_dir}/${subdir}/"; \
            fi; \
        done; \
    fi; \
    milvus-storage/java/patch_native_runpath.sh "${native_dir}"

# Build the milvus-storage Java binding consumed as an unmanaged JAR below.
RUN cd milvus-storage/java && bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh && sbt package"

# Build and optionally publish the runnable assembly as the primary Maven JAR.
ENV GIT_BRANCH=${GIT_BRANCH}
ENV SBT_OPTS="-Xmx4g -Xms2g"
ARG PUBLISH_TO_CENTRAL=true
RUN bash -c "source $SDKMAN_DIR/bin/sdkman-init.sh \
        && sbt 'compile; Test/compile; assembly' \
        && assembly_jar=\$(find target/scala-2.13 -maxdepth 1 -type f -name 'spark-connector-assembly-*.jar' -print -quit) \
        && test -n \"\$assembly_jar\" \
        && test -s \"\$assembly_jar\" \
        && sha256sum \"\$assembly_jar\" \
        && if [ \"$PUBLISH_TO_CENTRAL\" = \"true\" ]; then sbt publish; fi"

# Stage 2: retain only the built package for local inspection. The release
# pipeline publishes the Maven artifact and does not push this image.
FROM spark:4.0.1-scala2.13-java21-python3-ubuntu AS final

USER root
WORKDIR /opt/spark-milvus
COPY --from=builder /workspace/target/scala-2.13/spark-connector-assembly-*.jar ./

CMD ["/bin/bash"]
