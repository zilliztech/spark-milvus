// Helpers every JNI translation unit in this module shares: turning a
// LoonFFIResult into a Java exception, owning JNI string memory, and building a
// LoonProperties from a java.util.Map.
//
// Header-only and in an anonymous namespace per translation unit, so nothing
// here is exported from the shared library.

#ifndef ZILLIZ_NATIVE_STORAGE_JNI_COMMON_H_
#define ZILLIZ_NATIVE_STORAGE_JNI_COMMON_H_

#include <jni.h>

#include <string>
#include <vector>

#include "milvus-storage/ffi_c.h"

namespace {

constexpr const char* kExceptionClass =
    "com/zilliz/milvus/jni/storage/StorageNativeException";

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

bool Check(JNIEnv* env, LoonFFIResult result) {
  if (loon_ffi_is_success(&result)) {
    loon_ffi_free_result(&result);
    return true;
  }
  ThrowNative(env, &result);
  return false;
}

void ThrowIllegalArgument(JNIEnv* env, const char* message) {
  jclass cls = env->FindClass("java/lang/IllegalArgumentException");
  if (cls != nullptr) env->ThrowNew(cls, message);
}

class Utf8 {
 public:
  Utf8(JNIEnv* env, jstring value) : env_(env), value_(value) {
    chars_ =
        value_ != nullptr ? env_->GetStringUTFChars(value_, nullptr) : nullptr;
  }
  ~Utf8() {
    if (chars_ != nullptr) env_->ReleaseStringUTFChars(value_, chars_);
  }
  Utf8(const Utf8&) = delete;
  Utf8& operator=(const Utf8&) = delete;

  const char* c_str() const { return chars_; }

 private:
  JNIEnv* env_;
  jstring value_;
  const char* chars_;
};

// Builds LoonProperties from a java.util.Map<String,String> and frees it on
// scope exit.
class Properties {
 public:
  Properties() : value_{nullptr, 0}, created_(false) {}
  ~Properties() {
    if (created_) loon_properties_free(&value_);
  }
  Properties(const Properties&) = delete;
  Properties& operator=(const Properties&) = delete;

  bool Build(JNIEnv* env, jobject map) {
    if (map == nullptr) return true;

    jclass map_class = env->GetObjectClass(map);
    jmethodID entry_set =
        env->GetMethodID(map_class, "entrySet", "()Ljava/util/Set;");
    jobject set = env->CallObjectMethod(map, entry_set);
    if (env->ExceptionCheck()) return false;
    jclass set_class = env->GetObjectClass(set);
    jmethodID to_array =
        env->GetMethodID(set_class, "toArray", "()[Ljava/lang/Object;");
    auto entries = static_cast<jobjectArray>(env->CallObjectMethod(set, to_array));
    if (env->ExceptionCheck()) return false;

    jsize count = env->GetArrayLength(entries);
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(count);
    values.reserve(count);
    for (jsize i = 0; i < count; ++i) {
      jobject entry = env->GetObjectArrayElement(entries, i);
      jclass entry_class = env->GetObjectClass(entry);
      jmethodID get_key =
          env->GetMethodID(entry_class, "getKey", "()Ljava/lang/Object;");
      jmethodID get_value =
          env->GetMethodID(entry_class, "getValue", "()Ljava/lang/Object;");
      auto key = static_cast<jstring>(env->CallObjectMethod(entry, get_key));
      auto value = static_cast<jstring>(env->CallObjectMethod(entry, get_value));
      const char* key_chars = env->GetStringUTFChars(key, nullptr);
      const char* value_chars = env->GetStringUTFChars(value, nullptr);
      keys.emplace_back(key_chars != nullptr ? key_chars : "");
      values.emplace_back(value_chars != nullptr ? value_chars : "");
      if (key_chars != nullptr) env->ReleaseStringUTFChars(key, key_chars);
      if (value_chars != nullptr) env->ReleaseStringUTFChars(value, value_chars);
      env->DeleteLocalRef(entry);
    }

    std::vector<const char*> key_ptrs;
    std::vector<const char*> value_ptrs;
    key_ptrs.reserve(keys.size());
    value_ptrs.reserve(values.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      key_ptrs.push_back(keys[i].c_str());
      value_ptrs.push_back(values[i].c_str());
    }
    if (!Check(env, loon_properties_create(key_ptrs.data(), value_ptrs.data(),
                                           key_ptrs.size(), &value_))) {
      return false;
    }
    created_ = true;
    return true;
  }

  const LoonProperties* get() const { return created_ ? &value_ : nullptr; }

 private:
  LoonProperties value_;
  bool created_;
};

}  // namespace

#endif  // ZILLIZ_NATIVE_STORAGE_JNI_COMMON_H_
