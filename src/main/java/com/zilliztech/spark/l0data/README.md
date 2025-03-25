# L0 Delta Log Reader

This module provides tools for reading and analyzing Milvus L0 storage delta log files. These tools help in debugging and analyzing changes to the primary keys in Milvus collections.

## Overview

Delta logs are binary files that record operations (insertions, deletions, updates) performed on primary keys in a Milvus collection. These files use a format that includes:

- A magic number header to identify the file type
- A descriptor event containing metadata about the collection, partition, and segment
- A series of delta events that record changes to primary keys

This tool parses these files and extracts the primary keys and their associated timestamps.

## Key Components

### DeltaLogReader

The `DeltaLogReader` class is responsible for reading and parsing delta log files. It handles:

- Reading the file header and descriptor event
- Processing delta events (INSERT and DELETE events)
- Parsing the Parquet-formatted payload data

### ParquetPayloadReader

The `ParquetPayloadReader` class handles the parsing of Parquet-formatted payload data within delta events. It:

- Creates a temporary Parquet file from the binary data
- Uses the Apache Parquet library to read the file
- Extracts primary keys and timestamps based on the data type

### DeltaData

The `DeltaData` class stores the primary keys and their associated timestamps extracted from delta logs. It provides methods to:

- Append primary keys and timestamps
- Iterate through the stored data
- Get statistics about the data

### DeltaLogExample

The `DeltaLogExample` class provides a simple command-line interface for reading and analyzing delta log files. It displays:

- Metadata from the descriptor event
- Summary of the delta data entries
- Primary keys and their timestamps
- Timestamp distribution statistics

### DeltaLogUtils

The `DeltaLogUtils` class helps process delta logs with Apache Spark. It provides:

- Methods to convert delta logs to Spark DataFrames
- JSON parsing for extracting `pk` and `ts` from JSON-formatted primary keys
- Analysis utilities for comparing and processing multiple delta log files

### DeltaLogSparkExample

The `DeltaLogSparkExample` class demonstrates how to use Spark to analyze delta log data. It shows:

- Converting delta logs to DataFrames
- Basic DataFrame operations and statistics
- Analysis of timestamp distributions
- Comparing data across multiple delta log files

## Usage

### JSON Primary Key Parsing

The tool automatically parses JSON formatted primary keys. If a primary key is in JSON format like:

```json
{"pk":42,"ts":455257023810633736,"pkType":5}
```

It will extract these fields into the DataFrame columns:

- `pk`: The value of the "pk" field (in this example, 42)
- `ts`: The value of the "ts" field from the JSON string

If the JSON doesn't contain a `ts` field, the event timestamp will be used.

### Example Output

```
2023-07-20 15:28:32.045 [INFO] Reading file: src/main/resources/l0_delta_log_for_testing
2023-07-20 15:28:32.048 [INFO] Descriptor Event:
2023-07-20 15:28:32.048 [INFO]   Collection ID: 455256968555922472
2023-07-20 15:28:32.048 [INFO]   Partition ID: 455256968555922473
2023-07-20 15:28:32.048 [INFO]   Segment ID: 455256968556122495
2023-07-20 15:28:32.048 [INFO]   Field ID: -1
2023-07-20 15:28:32.048 [INFO]   Start Timestamp: 0
2023-07-20 15:28:32.048 [INFO]   End Timestamp: 0
2023-07-20 15:28:32.048 [INFO]   Payload Data Type: STRING
2023-07-20 15:28:32.048 [INFO]   Post Header Lengths: [52, 16, 16, 16, 16, 16, 16, 16]
2023-07-20 15:28:32.048 [INFO]   Extras: {original_size=2197}
...
```

### Spark DataFrame Output

```
+----+-------------+
|  pk|           ts|
+----+-------------+
|   0|455257023810633736|
|   1|455257023810633736|
|   2|455257023810633736|
|   3|455257023810633736|
|   4|455257023810633736|
+----+-------------+
```

## Programmatic Usage

You can also use the DeltaLogReader programmatically in your own applications:

```java
// Basic DeltaLogReader usage
try (DeltaLogReader reader = new DeltaLogReader("path/to/delta_log_file")) {
    List<DeltaData> deltaDataList = reader.readAll();
    
    // Process the data
    for (DeltaData deltaData : deltaDataList) {
        deltaData.range((pk, timestamp) -> {
            System.out.printf("PK: %s, Timestamp: %d%n", pk.getValue(), timestamp);
            return true; // Continue iteration
        });
    }
}

// Spark-based analysis
SparkSession spark = DeltaLogUtils.createLocalSparkSession("Delta Log Analyzer");
Dataset<Row> deltaDF = DeltaLogUtils.deltaLogToDataFrame("path/to/delta_log_file", spark);

// Filter based on pk
deltaDF.filter(col("pk").lt(10)).show();

// Get timestamp statistics
deltaDF.select(min("ts"), max("ts")).show();
```

## Logging Configuration

The logging level and format can be customized by modifying `src/main/resources/simplelogger.properties`. The default configuration sets:

- Default log level: INFO
- Package-specific log levels:
  - com.zilliz.spark.l0data: DEBUG
  - org.apache.hadoop: WARN
  - org.apache.parquet: WARN

## Known Limitations

- The current implementation focuses on STRING and INT64 primary key types
- Index file events are not fully processed
- Large delta log files may require significant memory
