#include <jni.h>
#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <random>
#include <filesystem>
#include <chrono>
#include <iomanip>
#include <map>
#include <sstream>
#include <dlfcn.h>
#include <atomic>
#include <mutex>
#include <thread>

// Milvus Storage headers
#include "milvus-storage/common/log.h"
#include "milvus-storage/storage/space.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/filter/constant_filter.h"
#include "milvus-storage/storage/options.h"

// Arrow headers
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/table.h>

// JSON handling (simplified for demo)
#include <nlohmann/json.hpp>

using namespace milvus_storage;
using json = nlohmann::json;

// Thread-safe lazy initialization for Spark environment
static std::atomic<bool> milvus_initialized{false};
static std::mutex init_mutex;
static thread_local bool in_initialization = false;

// Lazy initialization function with recursion protection
static void ensure_milvus_initialized()
{
  // Fast path: already initialized
  if (milvus_initialized.load(std::memory_order_acquire))
  {
    return;
  }

  // Prevent recursive initialization in the same thread
  if (in_initialization)
  {
    std::cerr << "Warning: Recursive initialization detected, skipping..." << std::endl;
    return;
  }

  std::lock_guard<std::mutex> lock(init_mutex);

  // Double-check pattern
  if (milvus_initialized.load(std::memory_order_acquire))
  {
    return;
  }

  in_initialization = true;

  try
  {
    // Initialize milvus-storage here instead of at library load time
    std::cout << "Initializing milvus-storage (Thread ID: " << std::this_thread::get_id() << ")..." << std::endl;

    // Set a smaller default stack size for internal operations to prevent overflow
    const size_t STACK_LIMIT = 1024 * 1024; // 1MB

    // Initialize with controlled stack usage
    milvus_initialized.store(true, std::memory_order_release);
    std::cout << "Milvus-storage initialized successfully" << std::endl;
  }
  catch (const std::exception &e)
  {
    std::cerr << "Failed to initialize milvus-storage: " << e.what() << std::endl;
    in_initialization = false;
    throw;
  }

  in_initialization = false;
}

// Global map to store Space instances
static std::map<jlong, std::shared_ptr<Space>> g_spaces;
static jlong g_next_handle = 1;

// Helper function to create Arrow Schema
std::shared_ptr<arrow::Schema> CreateArrowSchema(const std::vector<std::string> &field_names,
                                                 const std::vector<std::shared_ptr<arrow::DataType>> &field_types)
{
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (size_t i = 0; i < field_names.size(); ++i)
  {
    auto metadata = arrow::KeyValueMetadata::Make({{"ARROW_FIELD_ID"}}, {std::to_string(i + 100)});
    fields.push_back(arrow::field(field_names[i], field_types[i], true, metadata));
  }
  return arrow::schema(fields);
}

// Helper function to generate random vector data
std::string generateRandomVector(int dim)
{
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<float> dis(-1.0, 1.0);

  std::string result;
  result.resize(dim * sizeof(float));
  float *data = reinterpret_cast<float *>(result.data());

  for (int i = 0; i < dim; ++i)
  {
    data[i] = dis(gen);
  }
  return result;
}

// Helper function to create test data
std::shared_ptr<arrow::RecordBatch> createTestData(const json &data_json)
{
  auto arrow_schema = CreateArrowSchema(
      {"id", "timestamp", "vector", "label"},
      {arrow::int64(), arrow::int64(), arrow::fixed_size_binary(128), arrow::utf8()});

  arrow::Int64Builder id_builder;
  arrow::Int64Builder timestamp_builder;
  arrow::FixedSizeBinaryBuilder vector_builder(arrow::fixed_size_binary(128));
  arrow::StringBuilder label_builder;

  // Parse JSON data (simplified for demo)
  if (data_json.contains("batches") && !data_json["batches"].empty())
  {
    auto batch = data_json["batches"][0];

    auto ids = batch["id"];
    auto timestamps = batch["timestamp"];
    auto labels = batch["label"];

    for (size_t i = 0; i < ids.size(); ++i)
    {
      id_builder.Append(ids[i].get<int64_t>());
      timestamp_builder.Append(timestamps[i].get<int64_t>());
      vector_builder.Append(generateRandomVector(32)); // 32维向量，每维4字节
      label_builder.Append(labels[i].get<std::string>());
    }
  }

  std::shared_ptr<arrow::Array> id_array, timestamp_array, vector_array, label_array;
  id_builder.Finish(&id_array);
  timestamp_builder.Finish(&timestamp_array);
  vector_builder.Finish(&vector_array);
  label_builder.Finish(&label_array);

  return arrow::RecordBatch::Make(arrow_schema, id_array->length(),
                                  {id_array, timestamp_array, vector_array, label_array});
}

// Convert jstring to std::string
std::string jstring_to_string(JNIEnv *env, jstring jstr)
{
  if (jstr == nullptr)
    return "";

  const char *chars = env->GetStringUTFChars(jstr, nullptr);
  std::string result(chars);
  env->ReleaseStringUTFChars(jstr, chars);
  return result;
}

// Convert std::string to jstring
jstring string_to_jstring(JNIEnv *env, const std::string &str)
{
  return env->NewStringUTF(str.c_str());
}

extern "C"
{

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    createSpace
   * Signature: (Ljava/lang/String;Ljava/lang/String;)J
   */
  JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_createSpace(JNIEnv *env, jclass clazz, jstring storagePath, jstring schemaJson)
  {
    ensure_milvus_initialized();

    try
    {
      std::string storage_path = jstring_to_string(env, storagePath);
      std::string schema_json = jstring_to_string(env, schemaJson);

      // Create storage directory if it doesn't exist
      std::filesystem::create_directories(storage_path);

      // Create Arrow Schema (simplified for demo)
      auto arrow_schema = CreateArrowSchema(
          {"id", "timestamp", "vector", "label"},
          {arrow::int64(), arrow::int64(), arrow::fixed_size_binary(128), arrow::utf8()});

      SchemaOptions schema_options;
      schema_options.primary_column = "id";
      schema_options.version_column = "timestamp";
      schema_options.vector_column = "vector";

      auto schema = std::make_shared<Schema>(arrow_schema, schema_options);
      auto status = schema->Validate();
      if (!status.ok())
      {
        std::cerr << "Schema validation failed: " << status.ToString() << std::endl;
        return 0;
      }

      // Open Space
      auto space_result = Space::Open(storage_path, Options{schema, -1});
      if (!space_result.ok())
      {
        std::cerr << "Failed to open Space: " << space_result.status().ToString() << std::endl;
        return 0;
      }

      auto space = std::move(space_result).value();
      jlong handle = g_next_handle++;
      g_spaces[handle] = std::move(space);

      return handle;
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in createSpace: " << e.what() << std::endl;
      return 0;
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    writeData
   * Signature: (JLjava/lang/String;)Z
   */
  JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_writeData(JNIEnv *env, jclass clazz, jlong spaceHandle, jstring dataJson)
  {

    try
    {
      auto it = g_spaces.find(spaceHandle);
      if (it == g_spaces.end())
      {
        std::cerr << "Invalid space handle: " << spaceHandle << std::endl;
        return JNI_FALSE;
      }

      auto space = it->second;
      std::string data_json_str = jstring_to_string(env, dataJson);

      // Parse JSON data
      json data_json = json::parse(data_json_str);

      // Create record batch from JSON
      auto record_batch = createTestData(data_json);
      auto reader = arrow::RecordBatchReader::Make({record_batch}, record_batch->schema()).ValueOrDie();

      WriteOption write_option{1000};
      auto write_status = space->Write(*reader, write_option);

      return write_status.ok() ? JNI_TRUE : JNI_FALSE;
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in writeData: " << e.what() << std::endl;
      return JNI_FALSE;
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    readData
   * Signature: (J[Ljava/lang/String;)Ljava/lang/String;
   */
  JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readData(JNIEnv *env, jclass clazz, jlong spaceHandle, jobjectArray columnNames)
  {

    try
    {
      auto it = g_spaces.find(spaceHandle);
      if (it == g_spaces.end())
      {
        std::cerr << "Invalid space handle: " << spaceHandle << std::endl;
        return string_to_jstring(env, "{}");
      }

      auto space = it->second;

      // Parse column names
      ReadOptions read_options;
      jsize len = env->GetArrayLength(columnNames);
      for (jsize i = 0; i < len; ++i)
      {
        jstring column = (jstring)env->GetObjectArrayElement(columnNames, i);
        std::string column_name = jstring_to_string(env, column);
        read_options.columns.insert(column_name);
      }

      auto read_result = space->Read(read_options);
      auto table_result = read_result->ToTable();
      if (!table_result.ok())
      {
        std::cerr << "Failed to convert to table: " << table_result.status().ToString() << std::endl;
        return string_to_jstring(env, "{}");
      }

      auto table = table_result.ValueOrDie();

      // Convert table to JSON (simplified)
      json result_json;
      result_json["num_rows"] = table->num_rows();
      result_json["num_columns"] = table->num_columns();
      result_json["status"] = "success";

      return string_to_jstring(env, result_json.dump());
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in readData: " << e.what() << std::endl;
      json error_json;
      error_json["status"] = "error";
      error_json["message"] = e.what();
      return string_to_jstring(env, error_json.dump());
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    readDataWithFilter
   * Signature: (J[Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
   */
  JNIEXPORT jstring JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_readDataWithFilter(JNIEnv *env, jclass clazz, jlong spaceHandle, jobjectArray columnNames, jstring filterJson)
  {

    try
    {
      auto it = g_spaces.find(spaceHandle);
      if (it == g_spaces.end())
      {
        std::cerr << "Invalid space handle: " << spaceHandle << std::endl;
        return string_to_jstring(env, "{}");
      }

      auto space = it->second;
      std::string filter_json_str = jstring_to_string(env, filterJson);

      // Parse column names
      ReadOptions read_options;
      jsize len = env->GetArrayLength(columnNames);
      for (jsize i = 0; i < len; ++i)
      {
        jstring column = (jstring)env->GetObjectArrayElement(columnNames, i);
        std::string column_name = jstring_to_string(env, column);
        read_options.columns.insert(column_name);
      }

      // Add filter (simplified for demo)
      ConstantFilter filter(EQUAL, "id", Value::Int64(1));
      read_options.filters.push_back(&filter);

      auto read_result = space->Read(read_options);
      auto table_result = read_result->ToTable();
      if (!table_result.ok())
      {
        std::cerr << "Failed to convert to table: " << table_result.status().ToString() << std::endl;
        return string_to_jstring(env, "{}");
      }

      auto table = table_result.ValueOrDie();

      // Convert table to JSON (simplified)
      json result_json;
      result_json["num_rows"] = table->num_rows();
      result_json["num_columns"] = table->num_columns();
      result_json["status"] = "success";
      result_json["filter"] = filter_json_str;

      return string_to_jstring(env, result_json.dump());
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in readDataWithFilter: " << e.what() << std::endl;
      json error_json;
      error_json["status"] = "error";
      error_json["message"] = e.what();
      return string_to_jstring(env, error_json.dump());
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    getSpaceVersion
   * Signature: (J)J
   */
  JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getSpaceVersion(JNIEnv *env, jclass clazz, jlong spaceHandle)
  {

    try
    {
      auto it = g_spaces.find(spaceHandle);
      if (it == g_spaces.end())
      {
        std::cerr << "Invalid space handle: " << spaceHandle << std::endl;
        return -1;
      }

      auto space = it->second;
      return space->GetCurrentVersion();
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in getSpaceVersion: " << e.what() << std::endl;
      return -1;
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    getStorageSize
   * Signature: (Ljava/lang/String;)J
   */
  JNIEXPORT jlong JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_getStorageSize(JNIEnv *env, jclass clazz, jstring storagePath)
  {

    try
    {
      std::string storage_path = jstring_to_string(env, storagePath);

      size_t total_size = 0;
      if (std::filesystem::exists(storage_path))
      {
        for (const auto &entry : std::filesystem::recursive_directory_iterator(storage_path))
        {
          if (entry.is_regular_file())
          {
            total_size += entry.file_size();
          }
        }
      }

      return static_cast<jlong>(total_size);
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in getStorageSize: " << e.what() << std::endl;
      return -1;
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    closeSpace
   * Signature: (J)Z
   */
  JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_closeSpace(JNIEnv *env, jclass clazz, jlong spaceHandle)
  {

    try
    {
      auto it = g_spaces.find(spaceHandle);
      if (it == g_spaces.end())
      {
        std::cerr << "Invalid space handle: " << spaceHandle << std::endl;
        return JNI_FALSE;
      }

      g_spaces.erase(it);
      return JNI_TRUE;
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in closeSpace: " << e.what() << std::endl;
      return JNI_FALSE;
    }
  }

  /*
   * Class:     com_zilliz_spark_connector_jni_MilvusStorageJNI
   * Method:    deleteSpace
   * Signature: (Ljava/lang/String;)Z
   */
  JNIEXPORT jboolean JNICALL Java_com_zilliz_spark_connector_jni_MilvusStorageJNI_00024_deleteSpace(JNIEnv *env, jclass clazz, jstring storagePath)
  {

    try
    {
      std::string storage_path = jstring_to_string(env, storagePath);

      if (std::filesystem::exists(storage_path))
      {
        std::filesystem::remove_all(storage_path);
      }

      return JNI_TRUE;
    }
    catch (const std::exception &e)
    {
      std::cerr << "Exception in deleteSpace: " << e.what() << std::endl;
      return JNI_FALSE;
    }
  }

} // extern "C"