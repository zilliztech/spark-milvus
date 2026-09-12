// JNI for milvus-storage's loon_filesystem_* interface.
//
// One native method per C entry point. Handles cross as jlong; a LoonFFIResult
// that is not success becomes a StorageNativeException and no code reaches
// Java. Nothing here interprets a property key or names a cloud: which backend
// runs is fs.cloud_provider, resolved inside the C layer.

#include <jni.h>

#include <cstring>
#include <string>
#include <vector>

#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_filesystem_c.h"

namespace {

constexpr const char* kExceptionClass =
    "com/zilliz/milvus/jni/storage/StorageNativeException";
constexpr const char* kFileEntryClass =
    "com/zilliz/milvus/jni/storage/FileEntry";

void ThrowNative(JNIEnv* env, LoonFFIResult* result) {
  jclass cls = env->FindClass(kExceptionClass);
  if (cls == nullptr) {
    loon_ffi_free_result(result);
    return;
  }
  jmethodID ctor = env->GetMethodID(cls, "<init>", "(ILjava/lang/String;)V");
  const char* message = result->message != nullptr ? result->message : "";
  jstring jmessage = env->NewStringUTF(message);
  jobject exception =
      env->NewObject(cls, ctor, static_cast<jint>(result->err_code), jmessage);
  loon_ffi_free_result(result);
  if (exception != nullptr) {
    env->Throw(static_cast<jthrowable>(exception));
  }
}

// Returns false and throws when the call failed.
bool Check(JNIEnv* env, LoonFFIResult result) {
  if (loon_ffi_is_success(&result)) {
    loon_ffi_free_result(&result);
    return true;
  }
  ThrowNative(env, &result);
  return false;
}

// Owns the UTF-8 bytes of a jstring for as long as it is in scope.
class Utf8 {
 public:
  Utf8(JNIEnv* env, jstring value) : env_(env), value_(value) {
    chars_ = value_ != nullptr ? env_->GetStringUTFChars(value_, nullptr) : nullptr;
  }
  ~Utf8() {
    if (chars_ != nullptr) {
      env_->ReleaseStringUTFChars(value_, chars_);
    }
  }
  Utf8(const Utf8&) = delete;
  Utf8& operator=(const Utf8&) = delete;

  const char* c_str() const { return chars_ != nullptr ? chars_ : ""; }
  uint32_t size() const {
    return chars_ != nullptr ? static_cast<uint32_t>(std::strlen(chars_)) : 0;
  }

 private:
  JNIEnv* env_;
  jstring value_;
  const char* chars_;
};

// Builds LoonProperties from a java.util.Map<String,String>. Keys and values
// are copied into the vectors below so they outlive the JNI string handles.
class Properties {
 public:
  Properties() : properties_{nullptr, 0}, created_(false) {}

  ~Properties() {
    if (created_) {
      loon_properties_free(&properties_);
    }
  }
  Properties(const Properties&) = delete;
  Properties& operator=(const Properties&) = delete;

  bool Build(JNIEnv* env, jobject map) {
    std::vector<std::string> keys;
    std::vector<std::string> values;
    if (map != nullptr && !Collect(env, map, &keys, &values)) {
      return false;
    }

    std::vector<const char*> key_ptrs;
    std::vector<const char*> value_ptrs;
    key_ptrs.reserve(keys.size());
    value_ptrs.reserve(values.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      key_ptrs.push_back(keys[i].c_str());
      value_ptrs.push_back(values[i].c_str());
    }

    LoonFFIResult result = loon_properties_create(
        key_ptrs.data(), value_ptrs.data(), key_ptrs.size(), &properties_);
    if (!Check(env, result)) {
      return false;
    }
    created_ = true;
    return true;
  }

  const LoonProperties* get() const { return &properties_; }

 private:
  static bool Collect(JNIEnv* env,
                      jobject map,
                      std::vector<std::string>* keys,
                      std::vector<std::string>* values) {
    jclass map_class = env->GetObjectClass(map);
    jmethodID entry_set =
        env->GetMethodID(map_class, "entrySet", "()Ljava/util/Set;");
    jobject set = env->CallObjectMethod(map, entry_set);
    if (env->ExceptionCheck()) return false;

    jclass set_class = env->GetObjectClass(set);
    jmethodID to_array =
        env->GetMethodID(set_class, "toArray", "()[Ljava/lang/Object;");
    jobjectArray entries =
        static_cast<jobjectArray>(env->CallObjectMethod(set, to_array));
    if (env->ExceptionCheck()) return false;

    jsize count = env->GetArrayLength(entries);
    keys->reserve(count);
    values->reserve(count);
    for (jsize i = 0; i < count; ++i) {
      jobject entry = env->GetObjectArrayElement(entries, i);
      jclass entry_class = env->GetObjectClass(entry);
      jmethodID get_key =
          env->GetMethodID(entry_class, "getKey", "()Ljava/lang/Object;");
      jmethodID get_value =
          env->GetMethodID(entry_class, "getValue", "()Ljava/lang/Object;");

      jstring key = static_cast<jstring>(env->CallObjectMethod(entry, get_key));
      jstring value =
          static_cast<jstring>(env->CallObjectMethod(entry, get_value));
      {
        Utf8 k(env, key);
        Utf8 v(env, value);
        keys->emplace_back(k.c_str());
        values->emplace_back(v.c_str());
      }
      env->DeleteLocalRef(key);
      env->DeleteLocalRef(value);
      env->DeleteLocalRef(entry);
      env->DeleteLocalRef(entry_class);
    }
    env->DeleteLocalRef(entries);
    env->DeleteLocalRef(set);
    return true;
  }

  LoonProperties properties_;
  bool created_;
};

jbyteArray ToByteArray(JNIEnv* env, const uint8_t* data, uint64_t size) {
  jbyteArray out = env->NewByteArray(static_cast<jsize>(size));
  if (out == nullptr) return nullptr;
  if (size > 0) {
    env->SetByteArrayRegion(out, 0, static_cast<jsize>(size),
                            reinterpret_cast<const jbyte*>(data));
  }
  return out;
}

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_filesystemGet(
    JNIEnv* env, jclass, jobject properties_map, jstring path) {
  Properties properties;
  if (!properties.Build(env, properties_map)) return 0;

  Utf8 p(env, path);
  FileSystemHandle handle = 0;
  if (!Check(env, loon_filesystem_get(properties.get(), p.c_str(), p.size(),
                                      &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_filesystemDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_filesystem_destroy(static_cast<FileSystemHandle>(handle));
}

JNIEXPORT jbyteArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readFileAll(
    JNIEnv* env, jclass, jlong handle, jstring path) {
  Utf8 p(env, path);
  uint8_t* data = nullptr;
  uint64_t size = 0;
  if (!Check(env, loon_filesystem_read_file_all(
                      static_cast<FileSystemHandle>(handle), p.c_str(),
                      p.size(), &data, &size))) {
    return nullptr;
  }
  jbyteArray out = ToByteArray(env, data, size);
  std::free(data);
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writeFile(
    JNIEnv* env, jclass, jlong handle, jstring path, jbyteArray data) {
  Utf8 p(env, path);
  jsize size = data != nullptr ? env->GetArrayLength(data) : 0;
  std::vector<uint8_t> buffer(static_cast<size_t>(size));
  if (size > 0) {
    env->GetByteArrayRegion(data, 0, size,
                            reinterpret_cast<jbyte*>(buffer.data()));
  }
  Check(env, loon_filesystem_write_file(static_cast<FileSystemHandle>(handle),
                                        p.c_str(), p.size(), buffer.data(),
                                        static_cast<uint64_t>(size), nullptr, 0));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_deleteFile(
    JNIEnv* env, jclass, jlong handle, jstring path) {
  Utf8 p(env, path);
  Check(env, loon_filesystem_delete_file(static_cast<FileSystemHandle>(handle),
                                         p.c_str(), p.size()));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_createDir(
    JNIEnv* env, jclass, jlong handle, jstring path, jboolean recursive) {
  Utf8 p(env, path);
  Check(env, loon_filesystem_create_dir(static_cast<FileSystemHandle>(handle),
                                        p.c_str(), p.size(),
                                        recursive == JNI_TRUE));
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_fileSize(
    JNIEnv* env, jclass, jlong handle, jstring path) {
  Utf8 p(env, path);
  uint64_t size = 0;
  if (!Check(env, loon_filesystem_get_file_info(
                      static_cast<FileSystemHandle>(handle), p.c_str(),
                      p.size(), &size))) {
    return 0;
  }
  return static_cast<jlong>(size);
}

JNIEXPORT jobjectArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_listDir(
    JNIEnv* env, jclass, jlong handle, jstring path, jboolean recursive) {
  Utf8 p(env, path);
  LoonFileInfoList list{nullptr, 0};
  if (!Check(env, loon_filesystem_list_dir(static_cast<FileSystemHandle>(handle),
                                           p.c_str(), p.size(),
                                           recursive == JNI_TRUE, &list))) {
    return nullptr;
  }

  jclass entry_class = env->FindClass(kFileEntryClass);
  if (entry_class == nullptr) {
    loon_filesystem_free_file_info_list(&list);
    return nullptr;
  }
  jmethodID ctor =
      env->GetMethodID(entry_class, "<init>", "(Ljava/lang/String;ZJJ)V");
  jobjectArray out = env->NewObjectArray(static_cast<jsize>(list.count),
                                         entry_class, nullptr);
  if (out == nullptr) {
    loon_filesystem_free_file_info_list(&list);
    return nullptr;
  }

  for (uint32_t i = 0; i < list.count; ++i) {
    const LoonFileInfo& info = list.entries[i];
    std::string entry_path(info.path != nullptr ? info.path : "",
                           info.path != nullptr ? info.path_len : 0);
    jstring jpath = env->NewStringUTF(entry_path.c_str());
    jobject entry = env->NewObject(
        entry_class, ctor, jpath, info.is_dir ? JNI_TRUE : JNI_FALSE,
        static_cast<jlong>(info.size), static_cast<jlong>(info.mtime_ns));
    env->SetObjectArrayElement(out, static_cast<jsize>(i), entry);
    env->DeleteLocalRef(entry);
    env->DeleteLocalRef(jpath);
  }
  loon_filesystem_free_file_info_list(&list);
  return out;
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_openReader(
    JNIEnv* env, jclass, jlong handle, jstring path, jlong file_size) {
  Utf8 p(env, path);
  FileSystemReaderHandle reader = 0;
  if (!Check(env, loon_filesystem_open_reader(
                      static_cast<FileSystemHandle>(handle), p.c_str(),
                      p.size(), static_cast<uint64_t>(file_size), &reader))) {
    return 0;
  }
  return static_cast<jlong>(reader);
}

JNIEXPORT jbyteArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerReadAt(
    JNIEnv* env, jclass, jlong reader, jlong offset, jlong length) {
  if (length < 0) {
    return ToByteArray(env, nullptr, 0);
  }
  std::vector<uint8_t> buffer(static_cast<size_t>(length));
  if (!Check(env, loon_filesystem_reader_readat(
                      static_cast<FileSystemReaderHandle>(reader),
                      static_cast<uint64_t>(offset),
                      static_cast<uint64_t>(length), buffer.data()))) {
    return nullptr;
  }
  return ToByteArray(env, buffer.data(), static_cast<uint64_t>(length));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerDestroy(
    JNIEnv*, jclass, jlong reader) {
  loon_filesystem_reader_destroy(static_cast<FileSystemReaderHandle>(reader));
}

}  // extern "C"
