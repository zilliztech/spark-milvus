# Backfill API

The Backfill API provides a streamlined way to add new fields to existing Milvus collections by joining original data with new field values and writing per-segment binlog files.

## Quick Start

### 1. Prepare New Field Data

Create a Parquet file with schema `(pk, new_field1, new_field2, ...)`:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Backfill")
  .master("local[*]")
  .getOrCreate()

// Create new field data with primary key
val newFieldData = Seq(
  (1L, "value_1", 100),
  (2L, "value_2", 200),
  (3L, "value_3", 300)
).toDF("pk", "new_string_field", "new_int_field")

// Save to Parquet
val dataPath = "/tmp/new_field_data.parquet"
newFieldData.write.mode("overwrite").parquet(dataPath)
```

### 2. Configure and Execute

```scala
import com.zilliz.spark.connector.operations.backfill._

// Configure backfill
val config = BackfillConfig(
  // Milvus connection
  milvusUri = "http://localhost:19530",
  milvusToken = "root:Milvus",
  databaseName = "default",
  collectionName = "my_collection",
  
  // Primary key field ID to read
  pkFieldToRead = 100,
  
  // S3/Minio storage
  s3Endpoint = "localhost:9000",
  s3BucketName = "a-bucket",
  s3AccessKey = "minioadmin",
  s3SecretKey = "minioadmin",
  s3UseSSL = false,
  s3RootPath = "files",
  s3Region = "us-east-1",
  
  // Optional: tuning
  batchSize = 1024
)

// Execute backfill
val result = MilvusBackfill.run(
  spark = spark,
  backfillDataPath = dataPath,
  config = config
)

// Handle result
result match {
  case Right(success) =>
    println(success.summary)
    println(s"Segments processed: ${success.segmentsProcessed}")
    println(s"Total rows: ${success.totalRowsWritten}")
    println(s"Execution time: ${success.executionTimeSec}s")
    
  case Left(error) =>
    println(s"Backfill failed: ${error.message}")
    error.cause.foreach(_.printStackTrace())
}
```

## Configuration

### Required Parameters

- `milvusUri`: Milvus server URI (e.g., "http://localhost:19530")
- `collectionName`: Collection name to backfill
- `pkFieldToRead`: Primary key field ID (e.g., 100)
- `s3Endpoint`: S3/Minio endpoint (e.g., "localhost:9000")
- `s3BucketName`: S3 bucket name
- `s3AccessKey`: S3 access key
- `s3SecretKey`: S3 secret key

### Optional Parameters

- `milvusToken`: Authentication token (default: "")
- `databaseName`: Database name (default: "default")
- `partitionName`: Specific partition to backfill (default: None)
- `s3UseSSL`: Use SSL for S3 (default: false)
- `s3RootPath`: Root path in bucket (default: "files")
- `s3Region`: S3 region (default: "us-east-1")
- `batchSize`: Write batch size (default: 1024)
- `customOutputPath`: Custom S3 output path (default: None)

## Error Handling

The API returns `Either[BackfillError, BackfillResult]`:

```scala
result match {
  case Right(success) =>
    // Process successful result
    
  case Left(ConnectionError(msg, _)) =>
    println(s"Connection error: $msg")
    
  case Left(SchemaValidationError(msg, _)) =>
    println(s"Schema error: $msg")
    
  case Left(DataReadError(path, msg, _)) =>
    println(s"Read error at $path: $msg")
    
  case Left(WriteError(segmentId, path, msg, _)) =>
    println(s"Write error for segment $segmentId: $msg")
    
  case Left(error) =>
    println(s"Error: ${error.message}")
}
```

## Error Types

- `ConnectionError`: Failed to connect to Milvus or retrieve metadata
- `SchemaValidationError`: Schema incompatibility between original and new data
- `DataReadError`: Failed to read new field data or collection data
- `SegmentProcessingError`: Error processing a specific segment
- `WriteError`: Failed to write backfill data for a segment
- `SparkConfigError`: Spark configuration issue
- `UnexpectedError`: General unexpected error

## Result Structure

```scala
case class BackfillResult(
  success: Boolean,
  segmentsProcessed: Int,
  totalRowsWritten: Long,
  manifestPaths: Seq[String],
  segmentResults: Map[Long, SegmentBackfillResult],
  executionTimeMs: Long,
  collectionId: Long,
  partitionId: Long,
  newFieldNames: Seq[String]
)
```

### Helper Methods

```scala
// Formatted summary
success.summary

// Per-segment details
success.segmentSummary

// Check all segments succeeded
success.allSegmentsSuccessful

// Execution time in seconds
success.executionTimeSec
```

## Output Structure

Backfill data is written to:

```
s3://{bucket}/{root_path}/insert_log/{collectionID}/{partitionID}/{segmentID}/new_field/
└── binlog_0/
    └── task_{taskPartitionId}_{taskId}/
        └── *.parquet files
```

By default, the output path follows the Milvus binlog structure. You can customize it using `customOutputPath` parameter.

## Best Practices

1. **Schema Matching**: Ensure `pk` field type in new data matches collection's primary key type (Int64 or VarChar)
2. **Primary Key Field**: Use `pkFieldToRead` to specify the primary key field ID (typically 100)
   - This field is used for joining with new data
   - Only this field (plus segment_id and row_offset) is read from the collection to minimize data transfer
3. **Batch Size**: Adjust based on data size (default 1024 works for most cases)
   - Larger batches: faster but more memory usage
   - Smaller batches: slower but safer for large datasets
4. **Error Handling**: Always use pattern matching on `Either` result to handle all error cases
5. **Cleanup**: Remove temporary Parquet files after successful backfill
6. **Data Preparation**: Save new field data as Parquet for best performance and type safety

## Complete Example

```scala
import org.apache.spark.sql.SparkSession
import com.zilliz.spark.connector.operations.backfill._

object BackfillExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Backfill")
      .master("local[*]")
      .getOrCreate()

    try {
      // Step 1: Prepare new field data
      val newFieldData = Seq(
        (1L, "value_1"),
        (2L, "value_2"),
        (3L, "value_3")
      ).toDF("pk", "new_description")
      
      val dataPath = "/tmp/new_fields.parquet"
      newFieldData.write.mode("overwrite").parquet(dataPath)

      // Step 2: Configure
      val config = BackfillConfig(
        milvusUri = "http://localhost:19530",
        milvusToken = "root:Milvus",
        collectionName = "my_collection",
        pkFieldToRead = 100,
        s3Endpoint = "localhost:9000",
        s3BucketName = "a-bucket",
        s3AccessKey = "minioadmin",
        s3SecretKey = "minioadmin"
      )

      // Step 3: Execute
      val result = MilvusBackfill.backfill(spark, dataPath, config)

      // Step 4: Handle result
      result match {
        case Right(success) =>
          println("✓ Backfill completed!")
          println(success.summary)
          
        case Left(error) =>
          println(s"✗ Backfill failed: ${error.message}")
          System.exit(1)
      }

    } finally {
      spark.stop()
    }
  }
}
```

## Testing

Use the test helper for quick setup:

```scala
val config = BackfillConfig.forTest(
  collectionName = "test_collection",
  pkFieldToRead = 100
)
```

## How It Works

The backfill operation follows these steps:

1. **Validate Configuration**: Checks all required parameters are set
2. **Read New Field Data**: Loads Parquet file with `(pk, new_field1, ...)` schema
3. **Read Original Collection**: Reads only primary key field + metadata (segment_id, row_offset) from Milvus
4. **Get Primary Key Name**: Retrieves actual PK field name from Milvus schema (not hardcoded)
5. **Validate Schema**: Ensures PK types match between original and new data
6. **Perform Join**: Left joins original data with new field data on primary key
7. **Retrieve Metadata**: Gets collection ID and partition ID from Milvus
8. **Process Segments in Parallel**: For each segment:
   - Filters joined data for that segment
   - Sorts by row_offset to maintain original order
   - Writes new field binlog files to S3 using MilvusLoonWriter
9. **Return Results**: Provides detailed results including manifest paths

## Troubleshooting

### Schema validation error
- Ensure `pk` column exists in new field data
- Verify `pk` type matches collection's primary key type (use same type: Long for Int64, String for VarChar)
- Check that new field data has at least one field besides `pk`

### Connection error
- Check Milvus server is running and accessible at specified URI
- Verify Milvus token format is correct: "username:password"
- Verify S3/Minio endpoint is accessible and credentials are correct
- Check database and collection names are correct

### Write error
- Check S3 bucket exists and is writable
- Verify path permissions and bucket policies
- Ensure sufficient disk space on executors
- Check batch size isn't too large for available memory

## API Location

```
com.zilliz.spark.connector.operations.backfill.MilvusBackfill
com.zilliz.spark.connector.operations.backfill.BackfillConfig
com.zilliz.spark.connector.operations.backfill.BackfillResult
com.zilliz.spark.connector.operations.backfill.BackfillError
```
