# native-vector 的 C 源码

放 `mv_*` 的 C shim 与 JNI：`mv_*` 包 knowhere::Index、BruteForce、BinarySet、Version，以及 DiskANN 的本地 FileManager；JNI 侧对应 `com.zilliz.milvus.native.vector.jni.VectorNative`。C 头文件里不出现 JNI 类型，JNI 只在 `jni` 包（modules.md §4 第 2 条）。

构建脚本在模块实现时加；产物按 `native/{os}-{arch}/` 平铺进 jar。
