# Upstream storage JNI

The JNI implementation and Java/Scala API belong to the pinned `milvus-storage`
submodule, under `cpp/src/jni` and `java/src/main`. This connector does not
compile a separate C++ bridge or declare its own native methods.

The `native-storage` sbt module cross-compiles that upstream API for Scala 2.12
and 2.13. With a unified platform bundle, `NativeStorageLibrary` obtains the
verified `libmilvus-storage-jni` path from `native-runtime` and hands it to the
upstream `NativeLibraryLoader`; the upstream loader still performs
`System.load`. Without a unified bundle, the upstream `native/{os}-{arch}/`
resource and system-library-path behavior remains available.

See [the I/O design](../../../../docs/design/architecture/storage-io.html#upstream-jni)
for ownership, Arrow transfer and validation requirements.
