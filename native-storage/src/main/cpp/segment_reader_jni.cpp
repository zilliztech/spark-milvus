// JNI for milvus-storage's segment read path: column groups, the reader, and
// the per-batch RecordBatchReader the JVM consumes.
//
// Split from storage_native_jni.cpp because this file links Arrow C++ and that
// one does not. The reason is the per-batch reader below.
//
// ---------------------------------------------------------------------------
// Interim implementation, with a named end condition.
//
// milvus-storage already has this exact logic: loon_record_batch_reader_new /
// _read_next / _destroy, added by PR #493 specifically for this connector. But
// they are declared in ffi_jni.h without FFI_EXPORT and compiled only into
// libmilvus-storage-jni, the upstream JNI shim this module replaces. The
// library we link, libmilvus-storage, exports only the ArrowArrayStream form
// (loon_get_record_batch_reader), and that form is unusable from the JVM:
// Arrow Java's importer ignores ArrowArray.offset, so once the packed reader
// returns a sliced batch every later batch re-reads from buffer position 0.
//
// So this file rebuilds those three entry points on top of the stream. It goes
// away when upstream exports theirs from libmilvus-storage — requirement 7 in
// docs/design/architecture/storage-access.html. That is the end condition; the
// Arrow C++ dependency below exists only to satisfy it, and Arrow C++ has no
// stable ABI, so the headers here must match the Arrow statically linked into
// libmilvus-storage.
// ---------------------------------------------------------------------------

#include <jni.h>

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/bridge.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"

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

// An Arrow failure is not a LoonFFIResult, so it gets the same exception with
// the arrow error code the C layer uses for the same class of problem.
void ThrowArrow(JNIEnv* env, const arrow::Status& status) {
  jclass cls = env->FindClass(kExceptionClass);
  if (cls == nullptr) return;
  jmethodID ctor = env->GetMethodID(cls, "<init>", "(ILjava/lang/String;)V");
  jstring message = env->NewStringUTF(status.ToString().c_str());
  jobject exception =
      env->NewObject(cls, ctor, static_cast<jint>(loon_errcode_arrow), message);
  if (exception != nullptr) {
    env->Throw(static_cast<jthrowable>(exception));
  }
}

class Utf8 {
 public:
  Utf8(JNIEnv* env, jstring value) : env_(env), value_(value) {
    chars_ =
        value_ != nullptr ? env_->GetStringUTFChars(value_, nullptr) : nullptr;
  }
  ~Utf8() {
    if (chars_ != nullptr) {
      env_->ReleaseStringUTFChars(value_, chars_);
    }
  }
  Utf8(const Utf8&) = delete;
  Utf8& operator=(const Utf8&) = delete;

  const char* c_str() const { return chars_; }

 private:
  JNIEnv* env_;
  jstring value_;
  const char* chars_;
};

// Copies a Java String[] into owned std::strings plus a parallel array of
// pointers, which is the shape every loon_* entry point takes.
class StringArray {
 public:
  bool Build(JNIEnv* env, jobjectArray array) {
    if (array == nullptr) return true;
    jsize count = env->GetArrayLength(array);
    values_.reserve(count);
    for (jsize i = 0; i < count; ++i) {
      jstring element = static_cast<jstring>(env->GetObjectArrayElement(array, i));
      if (element == nullptr) {
        values_.emplace_back();
        continue;
      }
      const char* chars = env->GetStringUTFChars(element, nullptr);
      values_.emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);
    }
    pointers_.reserve(values_.size());
    for (const auto& value : values_) pointers_.push_back(value.c_str());
    return true;
  }

  const char** data() { return pointers_.empty() ? nullptr : pointers_.data(); }
  size_t size() const { return pointers_.size(); }

 private:
  std::vector<std::string> values_;
  std::vector<const char*> pointers_;
};

// Owns every allocation behind a LoonColumnGroups built from Java arrays. The
// C struct holds borrowed pointers, so this has to outlive the reader.
struct ColumnGroupsHolder {
  std::vector<std::vector<std::string>> columns;
  std::vector<std::vector<std::string>> paths;
  std::vector<std::vector<const char*>> column_ptrs;
  std::vector<std::vector<LoonColumnGroupFile>> files;
  std::vector<LoonColumnGroup> groups;
  std::string format;
  LoonColumnGroups value{};
};

struct RecordBatchReaderHolder {
  std::shared_ptr<arrow::RecordBatchReader> reader;
};

}  // namespace

extern "C" {

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_columnGroupsCreate(
    JNIEnv* env,
    jclass,
    jobjectArray columns_per_group,
    jobjectArray files_per_group,
    jobjectArray row_counts_per_group,
    jstring format) {
  if (columns_per_group == nullptr || files_per_group == nullptr ||
      row_counts_per_group == nullptr) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "columns, files and row counts must not be null"));
    return 0;
  }
  jsize group_count = env->GetArrayLength(columns_per_group);
  if (env->GetArrayLength(files_per_group) != group_count ||
      env->GetArrayLength(row_counts_per_group) != group_count) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "columns, files and row counts must have the same "
                        "number of column groups"));
    return 0;
  }

  auto holder = std::make_unique<ColumnGroupsHolder>();
  Utf8 fmt(env, format);
  holder->format = fmt.c_str() != nullptr ? fmt.c_str() : "parquet";
  holder->columns.resize(group_count);
  holder->paths.resize(group_count);
  holder->column_ptrs.resize(group_count);
  holder->files.resize(group_count);
  holder->groups.resize(group_count);

  for (jsize g = 0; g < group_count; ++g) {
    auto columns = static_cast<jobjectArray>(
        env->GetObjectArrayElement(columns_per_group, g));
    auto paths = static_cast<jobjectArray>(
        env->GetObjectArrayElement(files_per_group, g));
    auto counts = static_cast<jlongArray>(
        env->GetObjectArrayElement(row_counts_per_group, g));
    if (columns == nullptr || paths == nullptr || counts == nullptr) {
      ThrowArrow(env, arrow::Status::Invalid("column group ", g, " is null"));
      return 0;
    }

    jsize column_count = env->GetArrayLength(columns);
    for (jsize i = 0; i < column_count; ++i) {
      auto element = static_cast<jstring>(env->GetObjectArrayElement(columns, i));
      const char* chars = env->GetStringUTFChars(element, nullptr);
      holder->columns[g].emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);
    }
    for (const auto& column : holder->columns[g]) {
      holder->column_ptrs[g].push_back(column.c_str());
    }

    jsize file_count = env->GetArrayLength(paths);
    if (env->GetArrayLength(counts) != file_count) {
      ThrowArrow(env, arrow::Status::Invalid(
                          "column group ", g, " has ", file_count,
                          " files but ", env->GetArrayLength(counts),
                          " row counts"));
      return 0;
    }
    std::vector<jlong> row_counts(file_count);
    if (file_count > 0) {
      env->GetLongArrayRegion(counts, 0, file_count, row_counts.data());
    }

    // start_index/end_index are the file's row range within the column group,
    // so they accumulate across the group's files.
    int64_t start = 0;
    for (jsize f = 0; f < file_count; ++f) {
      auto element = static_cast<jstring>(env->GetObjectArrayElement(paths, f));
      const char* chars = env->GetStringUTFChars(element, nullptr);
      holder->paths[g].emplace_back(chars != nullptr ? chars : "");
      if (chars != nullptr) env->ReleaseStringUTFChars(element, chars);
      env->DeleteLocalRef(element);

      LoonColumnGroupFile file{};
      file.path = holder->paths[g].back().c_str();
      file.start_index = start;
      file.end_index = start + static_cast<int64_t>(row_counts[f]);
      file.property_keys = nullptr;
      file.property_values = nullptr;
      file.num_properties = 0;
      holder->files[g].push_back(file);
      start = file.end_index;
    }

    LoonColumnGroup& group = holder->groups[g];
    group.columns = holder->column_ptrs[g].empty()
                        ? nullptr
                        : holder->column_ptrs[g].data();
    group.num_of_columns = static_cast<uint32_t>(holder->column_ptrs[g].size());
    group.format = holder->format.c_str();
    group.files = holder->files[g].empty() ? nullptr : holder->files[g].data();
    group.num_of_files = static_cast<uint32_t>(holder->files[g].size());
  }

  holder->value.column_group_array =
      holder->groups.empty() ? nullptr : holder->groups.data();
  holder->value.num_of_column_groups = static_cast<uint32_t>(holder->groups.size());
  return reinterpret_cast<jlong>(holder.release());
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_columnGroupsDestroy(
    JNIEnv*, jclass, jlong handle) {
  delete reinterpret_cast<ColumnGroupsHolder*>(handle);
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerNew(
    JNIEnv* env,
    jclass,
    jlong column_groups,
    jlong arrow_schema_address,
    jobjectArray needed_columns,
    jobject properties) {
  auto* holder = reinterpret_cast<ColumnGroupsHolder*>(column_groups);
  if (holder == nullptr || arrow_schema_address == 0) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "column groups and arrow schema must not be null"));
    return 0;
  }

  StringArray columns;
  if (!columns.Build(env, needed_columns)) return 0;

  LoonProperties props{nullptr, 0};
  bool props_created = false;
  if (properties != nullptr) {
    // The property bag is built by the same helper storage_native_jni.cpp uses;
    // duplicated here rather than exported, because that file is deliberately
    // free of Arrow C++ and a shared header would drag this one's includes in.
    jclass map_class = env->GetObjectClass(properties);
    jmethodID entry_set =
        env->GetMethodID(map_class, "entrySet", "()Ljava/util/Set;");
    jobject set = env->CallObjectMethod(properties, entry_set);
    if (env->ExceptionCheck()) return 0;
    jclass set_class = env->GetObjectClass(set);
    jmethodID to_array =
        env->GetMethodID(set_class, "toArray", "()[Ljava/lang/Object;");
    auto entries =
        static_cast<jobjectArray>(env->CallObjectMethod(set, to_array));
    if (env->ExceptionCheck()) return 0;

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
    for (size_t i = 0; i < keys.size(); ++i) {
      key_ptrs.push_back(keys[i].c_str());
      value_ptrs.push_back(values[i].c_str());
    }
    if (!Check(env, loon_properties_create(key_ptrs.data(), value_ptrs.data(),
                                           key_ptrs.size(), &props))) {
      return 0;
    }
    props_created = true;
  }

  LoonReaderHandle reader = 0;
  bool ok = Check(env, loon_reader_new(
                           &holder->value,
                           reinterpret_cast<struct ArrowSchema*>(arrow_schema_address),
                           columns.data(), columns.size(),
                           props_created ? &props : nullptr, &reader));
  if (props_created) loon_properties_free(&props);
  return ok ? static_cast<jlong>(reader) : 0;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_readerDestroySegment(
    JNIEnv*, jclass, jlong reader) {
  loon_reader_destroy(static_cast<LoonReaderHandle>(reader));
}

JNIEXPORT jlong JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderNew(
    JNIEnv* env, jclass, jlong reader, jstring predicate) {
  Utf8 p(env, predicate);
  ArrowArrayStream stream{};
  if (!Check(env, loon_get_record_batch_reader(
                      static_cast<LoonReaderHandle>(reader), p.c_str(),
                      &stream))) {
    return 0;
  }
  auto imported = arrow::ImportRecordBatchReader(&stream);
  if (!imported.ok()) {
    if (stream.release != nullptr) stream.release(&stream);
    ThrowArrow(env, imported.status());
    return 0;
  }
  auto* holder = new RecordBatchReaderHolder{imported.MoveValueUnsafe()};
  return reinterpret_cast<jlong>(holder);
}

// Fills the caller-allocated ArrowArray/ArrowSchema with the next batch.
// Returns false at EOF, leaving both structs' release fields null.
//
// The offset fix is why this function exists. PackedRecordBatchReader::ReadNext
// can return a batch whose columns carry a non-zero offset: when a chunk is
// larger than min_rows it keeps the remainder via rb->Slice(min_rows). The C
// Data Interface says consumers must honour offset; Arrow Java does not. So a
// sliced column is materialized into a fresh offset-0 array, which copies only
// the slice range. Columns already at offset 0 pass through untouched.
JNIEXPORT jboolean JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderReadNext(
    JNIEnv* env, jclass, jlong handle, jlong array_address, jlong schema_address) {
  auto* holder = reinterpret_cast<RecordBatchReaderHolder*>(handle);
  auto* out_array = reinterpret_cast<struct ArrowArray*>(array_address);
  auto* out_schema = reinterpret_cast<struct ArrowSchema*>(schema_address);
  if (holder == nullptr || out_array == nullptr || out_schema == nullptr) {
    ThrowArrow(env, arrow::Status::Invalid(
                        "handle, array and schema must not be null"));
    return JNI_FALSE;
  }

  std::shared_ptr<arrow::RecordBatch> batch;
  arrow::Status status = holder->reader->ReadNext(&batch);
  if (!status.ok()) {
    ThrowArrow(env, status);
    return JNI_FALSE;
  }
  if (batch == nullptr) {
    out_array->release = nullptr;
    out_schema->release = nullptr;
    return JNI_FALSE;
  }

  bool sliced = false;
  for (int i = 0; i < batch->num_columns(); ++i) {
    if (batch->column(i)->offset() != 0) {
      sliced = true;
      break;
    }
  }
  if (sliced) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(batch->num_columns());
    for (int i = 0; i < batch->num_columns(); ++i) {
      auto column = batch->column(i);
      if (column->offset() == 0) {
        columns.push_back(column);
        continue;
      }
      auto materialized =
          arrow::Concatenate({column}, arrow::default_memory_pool());
      if (!materialized.ok()) {
        ThrowArrow(env, materialized.status());
        return JNI_FALSE;
      }
      columns.push_back(materialized.MoveValueUnsafe());
    }
    batch = arrow::RecordBatch::Make(batch->schema(), batch->num_rows(), columns);
  }

  arrow::Status exported = arrow::ExportRecordBatch(*batch, out_array, out_schema);
  if (!exported.ok()) {
    ThrowArrow(env, exported);
    return JNI_FALSE;
  }
  return JNI_TRUE;
}

JNIEXPORT void JNICALL
Java_com_zilliz_milvus_jni_storage_StorageNative_recordBatchReaderDestroy(
    JNIEnv*, jclass, jlong handle) {
  delete reinterpret_cast<RecordBatchReaderHolder*>(handle);
}

}  // extern "C"
