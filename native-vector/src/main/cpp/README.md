# native-vector C sources

Holds the `mv_*` C shim over knowhere and its JNI layer. `mv_*` wraps
`knowhere::Index`, BruteForce, BinarySet, Version, and the local FileManager
DiskANN needs. On the JNI side it maps to
`com.zilliz.milvus.jni.vector.VectorNative`.

No JNI type appears in a C header; JNI lives only in the `jni` package
(constraint 2, section 4 of docs/design/architecture/modules.md).

The build scripts land here when the module is implemented. The artifacts go
into the jar flattened under `native/{os}-{arch}/`.

knowhere has no C interface of its own, so this shim is the only cross-language
asset in layer 1 — design its `.so` and header for a second, non-JVM caller.
