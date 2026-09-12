// JNI for milvus-storage's write path: the dataset writer, the segment writer
// and the manifest transaction.
//
// Every entry point here is exported from libmilvus-storage, so unlike the
// per-batch reader in segment_reader_jni.cpp nothing is reimplemented and Arrow
// C++ is not needed: Arrow crosses as an address the caller exported.
//
// Two kinds of column-group handle travel through this file and they must not
// be mixed up:
//   - the one columnGroupsCreate returns, built in segment_reader_jni.cpp from
//     a Java layout, released with columnGroupsDestroy;
//   - the one writerClose and segmentWriterClose return, allocated by the C
//     layer, released with nativeColumnGroupsDestroy.
// Only the second kind is what a transaction appends.

#include <jni.h>

#include <cstring>
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

extern "C" {

// ==================== Dataset writer ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerNew(
    JNIEnv* env,
    jclass,
    jstring base_path,
    jlong arrow_schema_address,
    jobject properties) {
  if (arrow_schema_address == 0) {
    ThrowIllegalArgument(env, "arrow schema address must not be zero");
    return 0;
  }
  Utf8 path(env, base_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonWriterHandle handle = 0;
  if (!Check(env, loon_writer_new(
                      path.c_str(),
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      props.get(), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerWrite(
    JNIEnv* env, jclass, jlong handle, jlong arrow_array_address) {
  if (arrow_array_address == 0) {
    ThrowIllegalArgument(env, "arrow array address must not be zero");
    return;
  }
  Check(env, loon_writer_write(
                 static_cast<LoonWriterHandle>(handle),
                 reinterpret_cast<struct ArrowArray*>(arrow_array_address)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerFlush(
    JNIEnv* env, jclass, jlong handle) {
  Check(env, loon_writer_flush(static_cast<LoonWriterHandle>(handle)));
}

// Returns the LoonColumnGroups the write produced, as a handle the caller
// releases with nativeColumnGroupsDestroy. Zero when the call failed.
JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerClose(
    JNIEnv* env,
    jclass,
    jlong handle,
    jobjectArray meta_keys,
    jobjectArray meta_values) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  if (meta_keys != nullptr && meta_values != nullptr) {
    jsize count = env->GetArrayLength(meta_keys);
    if (env->GetArrayLength(meta_values) != count) {
      ThrowIllegalArgument(env, "metadata keys and values must be the same length");
      return 0;
    }
    for (jsize i = 0; i < count; ++i) {
      auto key = static_cast<jstring>(env->GetObjectArrayElement(meta_keys, i));
      auto value = static_cast<jstring>(env->GetObjectArrayElement(meta_values, i));
      const char* key_chars = env->GetStringUTFChars(key, nullptr);
      const char* value_chars = env->GetStringUTFChars(value, nullptr);
      keys.emplace_back(key_chars != nullptr ? key_chars : "");
      values.emplace_back(value_chars != nullptr ? value_chars : "");
      if (key_chars != nullptr) env->ReleaseStringUTFChars(key, key_chars);
      if (value_chars != nullptr) env->ReleaseStringUTFChars(value, value_chars);
    }
  }
  // The C signature takes char** rather than const char**, so the pointers are
  // copied out of the owning strings here; nothing downstream writes to them.
  std::vector<char*> key_ptrs;
  std::vector<char*> value_ptrs;
  key_ptrs.reserve(keys.size());
  value_ptrs.reserve(values.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    key_ptrs.push_back(const_cast<char*>(keys[i].c_str()));
    value_ptrs.push_back(const_cast<char*>(values[i].c_str()));
  }

  LoonColumnGroups* groups = nullptr;
  if (!Check(env, loon_writer_close(static_cast<LoonWriterHandle>(handle),
                                    key_ptrs.empty() ? nullptr : key_ptrs.data(),
                                    value_ptrs.empty() ? nullptr : value_ptrs.data(),
                                    static_cast<uint16_t>(key_ptrs.size()),
                                    &groups))) {
    return 0;
  }
  return reinterpret_cast<jlong>(groups);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_writerDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_writer_destroy(static_cast<LoonWriterHandle>(handle));
}

// ==================== Segment writer ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterNew(
    JNIEnv* env,
    jclass,
    jlong arrow_schema_address,
    jstring segment_path,
    jobject properties) {
  if (arrow_schema_address == 0) {
    ThrowIllegalArgument(env, "arrow schema address must not be zero");
    return 0;
  }
  Utf8 path(env, segment_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  // No LOB columns: TEXT is out of scope, see capabilities.md section 10.
  LoonSegmentWriterConfig config{};
  config.segment_path = path.c_str();
  config.lob_columns = nullptr;
  config.num_lob_columns = 0;

  LoonSegmentWriterHandle handle = 0;
  if (!Check(env, loon_segment_writer_new(
                      reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                      &config, props.get(), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterWrite(
    JNIEnv* env, jclass, jlong handle, jlong arrow_array_address) {
  if (arrow_array_address == 0) {
    ThrowIllegalArgument(env, "arrow array address must not be zero");
    return;
  }
  Check(env,
        loon_segment_writer_write(
            static_cast<LoonSegmentWriterHandle>(handle),
            reinterpret_cast<struct ArrowArray*>(arrow_array_address)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterFlush(
    JNIEnv* env, jclass, jlong handle) {
  Check(env, loon_segment_writer_flush(
                 static_cast<LoonSegmentWriterHandle>(handle)));
}

// Fills a two-element long[]: the column groups handle and the row count. One
// array rather than two calls because close can only be called once.
JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterClose(
    JNIEnv* env, jclass, jlong handle) {
  LoonSegmentWriteOutput output{};
  if (!Check(env, loon_segment_writer_close(
                      static_cast<LoonSegmentWriterHandle>(handle), &output))) {
    return nullptr;
  }
  jlong values[2] = {reinterpret_cast<jlong>(output.column_groups),
                     static_cast<jlong>(output.rows_written)};
  jlongArray out = env->NewLongArray(2);
  if (out != nullptr) env->SetLongArrayRegion(out, 0, 2, values);
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_segmentWriterDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_segment_writer_destroy(static_cast<LoonSegmentWriterHandle>(handle));
}

// ==================== Transaction ====================

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionBegin(
    JNIEnv* env,
    jclass,
    jstring base_path,
    jobject properties,
    jlong read_version,
    jint resolve_id,
    jint retry_limit) {
  Utf8 path(env, base_path);
  Properties props;
  if (!props.Build(env, properties)) return 0;

  LoonTransactionHandle handle = 0;
  if (!Check(env, loon_transaction_begin(
                      path.c_str(), props.get(),
                      static_cast<int64_t>(read_version),
                      static_cast<int32_t>(resolve_id),
                      static_cast<uint32_t>(retry_limit), &handle))) {
    return 0;
  }
  return static_cast<jlong>(handle);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionCommit(
    JNIEnv* env, jclass, jlong handle) {
  int64_t version = 0;
  if (!Check(env, loon_transaction_commit(
                      static_cast<LoonTransactionHandle>(handle), &version))) {
    return -1;
  }
  return static_cast<jlong>(version);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionReadVersion(
    JNIEnv* env, jclass, jlong handle) {
  int64_t version = 0;
  if (!Check(env, loon_transaction_get_read_version(
                      static_cast<LoonTransactionHandle>(handle), &version))) {
    return -1;
  }
  return static_cast<jlong>(version);
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionDropColumn(
    JNIEnv* env, jclass, jlong handle, jstring column) {
  Utf8 name(env, column);
  Check(env, loon_transaction_drop_column(
                 static_cast<LoonTransactionHandle>(handle), name.c_str()));
}

// Appends every group of a native LoonColumnGroups, which is the shape
// writerClose and segmentWriterClose hand back.
JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAppendFiles(
    JNIEnv* env, jclass, jlong handle, jlong native_column_groups) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr) {
    ThrowIllegalArgument(env, "column groups must not be null");
    return;
  }
  Check(env, loon_transaction_append_files(
                 static_cast<LoonTransactionHandle>(handle), groups));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAddColumnGroup(
    JNIEnv* env, jclass, jlong handle, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return;
  }
  Check(env, loon_transaction_add_column_group(
                 static_cast<LoonTransactionHandle>(handle),
                 &groups->column_group_array[index]));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionAddDeltaLog(
    JNIEnv* env, jclass, jlong handle, jstring path, jlong num_entries) {
  Utf8 log(env, path);
  Check(env, loon_transaction_add_delta_log(
                 static_cast<LoonTransactionHandle>(handle), log.c_str(),
                 static_cast<int64_t>(num_entries)));
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_transactionDestroy(
    JNIEnv*, jclass, jlong handle) {
  loon_transaction_destroy(static_cast<LoonTransactionHandle>(handle));
}

// ==================== Reading back a native LoonColumnGroups ====================

JNIEXPORT jint JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupsCount(
    JNIEnv*, jclass, jlong native_column_groups) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  return groups == nullptr ? 0
                           : static_cast<jint>(groups->num_of_column_groups);
}

JNIEXPORT jobjectArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupFiles(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  jclass string_class = env->FindClass("java/lang/String");
  jobjectArray out =
      env->NewObjectArray(static_cast<jsize>(group.num_of_files), string_class, nullptr);
  if (out == nullptr) return nullptr;
  for (uint32_t i = 0; i < group.num_of_files; ++i) {
    const char* path = group.files[i].path;
    jstring value = env->NewStringUTF(path != nullptr ? path : "");
    env->SetObjectArrayElement(out, static_cast<jsize>(i), value);
    env->DeleteLocalRef(value);
  }
  return out;
}

JNIEXPORT jlongArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupRowCounts(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  std::vector<jlong> counts(group.num_of_files);
  for (uint32_t i = 0; i < group.num_of_files; ++i) {
    counts[i] = static_cast<jlong>(group.files[i].end_index -
                                   group.files[i].start_index);
  }
  jlongArray out = env->NewLongArray(static_cast<jsize>(counts.size()));
  if (out != nullptr && !counts.empty()) {
    env->SetLongArrayRegion(out, 0, static_cast<jsize>(counts.size()),
                            counts.data());
  }
  return out;
}

JNIEXPORT jobjectArray JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupColumns(
    JNIEnv* env, jclass, jlong native_column_groups, jint index) {
  auto* groups = reinterpret_cast<LoonColumnGroups*>(native_column_groups);
  if (groups == nullptr || index < 0 ||
      static_cast<uint32_t>(index) >= groups->num_of_column_groups) {
    ThrowIllegalArgument(env, "column group index is out of range");
    return nullptr;
  }
  const LoonColumnGroup& group = groups->column_group_array[index];
  jclass string_class = env->FindClass("java/lang/String");
  jobjectArray out = env->NewObjectArray(
      static_cast<jsize>(group.num_of_columns), string_class, nullptr);
  if (out == nullptr) return nullptr;
  for (uint32_t i = 0; i < group.num_of_columns; ++i) {
    const char* column = group.columns[i];
    jstring value = env->NewStringUTF(column != nullptr ? column : "");
    env->SetObjectArrayElement(out, static_cast<jsize>(i), value);
    env->DeleteLocalRef(value);
  }
  return out;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_nativeColumnGroupsDestroy(
    JNIEnv*, jclass, jlong native_column_groups) {
  loon_column_groups_destroy(
      reinterpret_cast<LoonColumnGroups*>(native_column_groups));
}

}  // extern "C"
