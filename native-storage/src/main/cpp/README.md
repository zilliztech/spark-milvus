# Upstream storage JNI

The JNI implementation and Java/Scala API belong to the pinned `milvus-storage`
submodule, under `cpp/src/jni` and `java/src/main`. This connector does not
compile a separate C++ bridge or declare its own native methods.

The `native-storage` sbt module cross-compiles that upstream API for Scala 2.12
and 2.13. Native builds produce `libmilvus-storage-jni`; resources retain the
upstream `native/{os}-{arch}/` layout and are loaded by its `NativeLibraryLoader`.

See [the I/O design](../../../../docs/design/architecture/storage-io.html#upstream-jni)
for ownership, Arrow transfer and validation requirements.
