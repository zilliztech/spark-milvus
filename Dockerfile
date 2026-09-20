# syntax=docker/dockerfile:1.4
# Build spark-milvus with the native resources selected for the worker platform

# Build arguments
ARG GIT_BRANCH=unknown
ARG TARGETARCH
ARG MAVEN_SNAPSHOT_REPOSITORY_URL=https://central.sonatype.com/repository/maven-snapshots/
ARG MAVEN_CREDENTIALS_FILE=/run/secrets/maven_credentials
ARG NATIVE_JOBS=50
ARG NATIVE_BUILD_OPTIONS
# Optional prebuilt JAR and .properties sidecar inside the build context.
ARG NATIVE_BUNDLE

# Stage 1: Build the unified native bundle and the connector
FROM spark:4.0.1-scala2.13-java21-python3-ubuntu AS builder

ARG GIT_BRANCH
ARG TARGETARCH
ARG MAVEN_SNAPSHOT_REPOSITORY_URL
ARG MAVEN_CREDENTIALS_FILE
ARG NATIVE_JOBS
ARG NATIVE_BUILD_OPTIONS
ARG NATIVE_BUNDLE

USER root

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# The unified native source profile pins GCC 12, including OpenBLAS's Fortran compiler.
# Rust bindgen also loads libclang to generate the storage bridge's C bindings.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget curl git g++ gcc gcc-12 g++-12 gfortran-12 make ccache gdb \
    python3 python3-pip \
    zip unzip pkg-config ninja-build \
    automake autoconf libtool patchelf libaio-dev libclang-dev \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/aclocal-1.16 /usr/bin/aclocal-1.15 \
    && ln -sf /usr/bin/automake-1.16 /usr/bin/automake-1.15

# Builds run on architecture-native workers. TARGETARCH is optional for
# backwards compatibility, but when supplied it must match the worker.
RUN set -eux; \
    case "$(uname -m)" in \
        x86_64|amd64) detected_arch=amd64 ;; \
        aarch64|arm64) detected_arch=arm64 ;; \
        *) echo "Unsupported build architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    requested_arch="${TARGETARCH:-${detected_arch}}"; \
    case "${requested_arch}" in amd64|arm64) ;; *) echo "Unsupported target architecture: ${requested_arch}" >&2; exit 1 ;; esac; \
    if [ "${requested_arch}" != "${detected_arch}" ]; then \
        echo "Build architecture ${detected_arch} does not match target ${requested_arch}" >&2; \
        exit 1; \
    fi

# Install CMake (architecture-aware, same pattern as milvus-storage)
RUN wget -qO- "https://cmake.org/files/v3.27/cmake-3.27.5-linux-$(uname -m).tar.gz" | tar --strip-components=1 -xz -C /usr/local

# The pinned milvus-storage submodule requires Conan 2.
ENV CONAN_HOME=/root/.conan2
RUN pip3 install --no-cache-dir conan==2.25.1

# The current milvus-storage format bridge is built from Rust sources.
ENV RUSTUP_HOME=/root/.rustup
ENV CARGO_HOME=/root/.cargo
ENV PATH=/root/.cargo/bin:${PATH}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable

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

# Initialize missing submodules without resetting source revisions copied from the build context.
RUN git config --global --add safe.directory /workspace && \
    git config --global --add safe.directory /workspace/milvus-proto && \
    git config --global --add safe.directory /workspace/milvus-storage && \
    git config --global --add safe.directory /workspace/knowhere && \
    make init-missing-submodules

# Linux x86_64 builds both engines; arm64 retains the existing storage build
# unless a matching prebuilt unified bundle is explicitly supplied.
# Cache dependencies across failed build steps. Initialize Conan after mounting
# its cache so an empty cache has the required profile and artifact remote.
RUN --mount=type=cache,id=spark-milvus-conan-2-${TARGETARCH},target=/root/.conan2,sharing=locked \
    --mount=type=cache,id=spark-milvus-cargo-registry,target=/root/.cargo/registry,sharing=locked \
    --mount=type=cache,id=spark-milvus-cargo-git,target=/root/.cargo/git,sharing=locked \
    --mount=type=cache,id=spark-milvus-ccache,target=/root/.ccache,sharing=locked \
    conan profile detect --force \
    && conan remote add --force default-conan-local2 \
        https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2 \
    && make native-resources "NATIVE_JOBS=${NATIVE_JOBS}" \
    "NATIVE_BUILD_OPTIONS=${NATIVE_BUILD_OPTIONS}" "NATIVE_BUNDLE=${NATIVE_BUNDLE}"

# Build and optionally publish the runnable assembly as the primary Maven JAR.
ENV GIT_BRANCH=${GIT_BRANCH}
ENV MAVEN_SNAPSHOT_REPOSITORY_URL=${MAVEN_SNAPSHOT_REPOSITORY_URL}
ENV MAVEN_CREDENTIALS_FILE=${MAVEN_CREDENTIALS_FILE}
ENV SBT_OPTS="-Xmx4g -Xms2g"
# PUBLISH_TO_CENTRAL is retained while Jenkins migrates to the repository-neutral flag.
ARG PUBLISH_MAVEN
ARG PUBLISH_TO_CENTRAL=true
RUN --mount=type=cache,id=spark-milvus-coursier,target=/root/.cache/coursier,sharing=locked \
    --mount=type=cache,id=spark-milvus-ivy2,target=/root/.ivy2/cache,sharing=locked \
    --mount=type=cache,id=spark-milvus-sbt,target=/root/.sbt,sharing=locked \
    --mount=type=secret,id=maven_credentials,target=/run/secrets/maven_credentials,required=false \
    set -eux; \
    case "$(uname -m)" in \
        x86_64|amd64) native_platform=linux-x86_64 ;; \
        aarch64|arm64) native_platform=linux-aarch64 ;; \
        *) echo "Unsupported build architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    set --; \
    if [ -n "${NATIVE_BUNDLE:-}" ] || [ "${native_platform}" = linux-x86_64 ]; then \
        native_bundle="${NATIVE_BUNDLE:-/workspace/target/native-build/${native_platform}/milvus-native-${native_platform}.jar}"; \
        native_bundle="$(readlink -f "${native_bundle}")"; \
        test -s "${native_bundle}"; \
        test -s "${native_bundle}.properties"; \
        set -- "-Dmilvus.native.bundle=${native_bundle}"; \
    fi; \
    sbt "$@" "compile; Test/compile; integration40/Test/compile; assembly"; \
    assembly_jar="$(find target/scala-2.13 -maxdepth 1 -type f -name 'spark-connector-assembly-*.jar' -print -quit)"; \
    test -n "${assembly_jar}"; \
    test -s "${assembly_jar}"; \
    entries_file="$(mktemp)"; \
    manifest_file="$(mktemp)"; \
    jar tf "${assembly_jar}" > "${entries_file}"; \
    if [ "$#" -gt 0 ]; then \
        resource_prefix="native/milvus/1/${native_platform}/"; \
        grep -Fqx "${resource_prefix}manifest.properties" "${entries_file}"; \
        unzip -p "${assembly_jar}" "${resource_prefix}manifest.properties" > "${manifest_file}"; \
        for entry in libmilvus-storage-jni.so libknowhere_jni.so; do \
            if ! grep -Fqx "${resource_prefix}${entry}" "${entries_file}"; then \
                canonical="$(awk -F= -v key="alias.${entry}" '$1 == key { print $2 }' "${manifest_file}")"; \
                test -n "${canonical}"; \
                grep -Fqx "${resource_prefix}${canonical}" "${entries_file}"; \
            fi; \
        done; \
        if grep -Eq '^native/(knowhere/|linux-[^/]+/)' "${entries_file}"; then \
            echo "Assembly unexpectedly contains legacy native resources" >&2; \
            exit 1; \
        fi; \
    else \
        for entry in libmilvus-storage.so libmilvus-storage-jni.so; do \
            grep -Fqx "native/${native_platform}/${entry}" "${entries_file}"; \
        done; \
    fi; \
    rm -f "${entries_file}" "${manifest_file}"; \
    sha256sum "${assembly_jar}"; \
    publish_maven="${PUBLISH_MAVEN:-${PUBLISH_TO_CENTRAL}}"; \
    case "${publish_maven}" in true|false) ;; *) echo "PUBLISH_MAVEN must be true or false" >&2; exit 1 ;; esac; \
    if [ "${publish_maven}" = "true" ]; then \
        test -s "${MAVEN_CREDENTIALS_FILE}"; \
        sbt "$@" publish; \
    fi

# Stage 2: retain only the built package for local inspection. The release
# pipeline publishes the Maven artifact and does not push this image.
FROM spark:4.0.1-scala2.13-java21-python3-ubuntu AS final

USER root
WORKDIR /opt/spark-milvus
COPY --from=builder /workspace/target/scala-2.13/spark-connector-assembly-*.jar ./

CMD ["/bin/bash"]
