# native-storage 的 C 源码

放 JNI 源码：milvus-storage 的 `loon_*` C 接口的封装，一个 `loon_*` 对应 `com.zilliz.milvus.native.storage.jni.StorageNative` 的一个 native 方法，句柄是 long，结果码转异常。C 头文件里不出现 JNI 类型，JNI 只在 `jni` 包（modules.md §4 第 2 条）。

构建脚本在模块实现时加，放 `native/storage/build/`（构建与 patchelf 脚本）；产物按 `native/{os}-{arch}/` 平铺进 jar。
