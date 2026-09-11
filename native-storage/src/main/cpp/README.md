# native-storage C sources

Holds the JNI sources that wrap milvus-storage's `loon_*` C interface. Each
`loon_*` entry point maps to one native method on
`com.zilliz.milvus.jni.storage.StorageNative`; handles are longs and result
codes become exceptions.

No JNI type appears in a C header; JNI lives only in the `jni` package
(constraint 2, section 4 of docs/design/modules.md).

The build and patchelf scripts land here when the module is implemented. The
artifacts go into the jar flattened under `native/{os}-{arch}/`.
