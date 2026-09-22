# native-vector native code ownership

Knowhere PR [#1829](https://github.com/zilliztech/knowhere/pull/1829) supplies
the C interface, JNI, Java API and native resource loader. The root `knowhere`
submodule follows `Thor-ChenBiao/knowhere-contrib` branch
`spark-milvus/knowhere-jni-pr`, which is the PR branch
(`LawrenceTL92/knowhere-contrib` `codex/knowhere-jni-pr`) plus two binding
additions offered to it as LawrenceTL92/knowhere-contrib#1; once that branch
holds them, the submodule points back at it. The superproject gitlink pins the
exact commit used by the build.

The connector consumes these artifacts through `NativeVectorLibrary` in
`com.zilliz.milvus.jni.vector`. Calling the upstream
`io.knowhere.Knowhere.cAbiVersion()` triggers its loader; the connector verifies
C ABI version 1 and exposes version information. It does not maintain another
C shim, JNI declaration or native resource extractor.

Keep upstream resources under `native/knowhere/1/<platform>/`. The upstream
loader owns platform selection, checksums, extraction and dependency loading.
Do not relocate the `io.knowhere` Java package when building an assembly.
Preload the JRE's `libjsig` before starting the JVM, as required by this build.

Persisted Milvus index decoding and search are subsequent work. The governing
design is [vector-search.html](../../../../docs/design/architecture/vector-search.html#library-loading).
