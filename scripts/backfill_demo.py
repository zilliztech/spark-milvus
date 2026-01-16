#!/usr/bin/env python3
"""
PySpark Backfill Demo Script

This script demonstrates the Milvus backfill operation using PySpark.
It creates a test collection, inserts data, and performs a backfill operation
to add new fields with NULL value handling.

Prerequisites:
- Milvus 2.6+ running at localhost:19530
- Minio running at localhost:9000
- PySpark installed
- pymilvus installed

Usage:
    python scripts/backfill_demo.py
"""

import os
import sys
import time
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
    MilvusClient
)
import numpy as np


class MilvusBackfillDemo:
    """Demo class for Milvus backfill operation"""

    def __init__(self):
        self.now = int(time.time() * 1000)
        self.collection_name = f"backfilltestcollection_{self.now}"
        self.snapshot_name = f"backfill_snapshot_{self.now}"
        self.dim = 128
        self.batch_size = 10000
        self.batch_count = 5
        self.total_records = self.batch_size * self.batch_count

        # Milvus connection params
        self.milvus_uri = "http://localhost:19530"
        self.milvus_token = "root:Milvus"

        # S3/Minio params
        self.s3_endpoint = "localhost:9000"
        self.s3_bucket = "a-bucket"
        self.s3_access_key = "minioadmin"
        self.s3_secret_key = "minioadmin"

        self.spark = None
        self.collection = None
        self.client = None

    def setup_spark(self):
        """Initialize SparkSession"""
        print("Initializing Spark session...")
        self.spark = SparkSession.builder \
            .appName("MilvusBackfillDemo") \
            .master("local[*]") \
            .config("spark.jars.packages", "io.milvus:spark-milvus_2.12:0.1.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        print(f"Spark version: {self.spark.version}")

    def setup_milvus(self):
        """Connect to Milvus and create test collection"""
        print("\nConnecting to Milvus...")
        connections.connect(
            alias="default",
            uri=self.milvus_uri,
            token=self.milvus_token
        )

        # Initialize MilvusClient for snapshot operations
        self.client = MilvusClient(
            uri=self.milvus_uri,
            token=self.milvus_token
        )

        # Drop collection if exists
        if utility.has_collection(self.collection_name):
            print(f"Dropping existing collection: {self.collection_name}")
            utility.drop_collection(self.collection_name)

        print(f"Creating collection: {self.collection_name}")
        self._create_collection()

    def _create_collection(self):
        """Create test collection with schema"""
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema(name="int64", dtype=DataType.INT64, is_clustering_key=True),
            FieldSchema(name="float", dtype=DataType.FLOAT),
            FieldSchema(name="varchar", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=self.dim)
        ]

        schema = CollectionSchema(
            fields=fields,
            description="Test collection for Milvus Backfill demo"
        )

        self.collection = Collection(
            name=self.collection_name,
            schema=schema,
            shards_num=1
        )

        print(f"Collection created: {self.collection_name}")

    def insert_test_data(self):
        """Insert test data into collection"""
        print(f"\nInserting {self.total_records:,} test records...")
        print(f"  Batches: {self.batch_count}")
        print(f"  Batch size: {self.batch_size:,}")

        np.random.seed(42)

        for i in range(self.batch_count):
            # Generate batch data
            ids = list(range(i * self.batch_size, (i + 1) * self.batch_size))
            int64s = list(range(self.batch_size))
            floats = np.random.random(self.batch_size).tolist()
            varchars = [f"test_string_{i * self.batch_size + j}" for j in range(self.batch_size)]
            vectors = np.random.random((self.batch_size, self.dim)).tolist()

            # Insert batch
            self.collection.insert([ids, int64s, floats, varchars, vectors])

            if (i + 1) % 10 == 0:
                print(f"  Inserted {(i + 1) * self.batch_size:,} records...")

        # Flush to ensure data is persisted
        print("  Flushing data...")
        self.collection.flush()
        time.sleep(3)  # Wait for flush to complete

        print(f"✓ Inserted {self.total_records:,} records successfully")

    def create_backfill_data(self):
        """Create backfill data with NULL gaps in the middle"""
        print("\n=== Creating Backfill Data ===")

        # Distribution:
        # - First 30%: Has backfill data
        # - Middle 40%: NULL (un-joined) ← Key test: NULL in the middle
        # - Last 30%: Has backfill data
        first_range_end = int(self.total_records * 0.3)
        null_range_start = first_range_end
        null_range_end = int(self.total_records * 0.7)
        last_range_start = null_range_end

        records_with_data = first_range_end + (self.total_records - last_range_start)
        expected_null_records = null_range_end - null_range_start

        print(f"Total records in collection: {self.total_records:,}")
        print("Backfill data distribution:")
        print(f"  Records 0-{first_range_end-1:,}: Has backfill data ({first_range_end:,} rows)")
        print(f"  Records {null_range_start:,}-{null_range_end-1:,}: NO backfill data ({expected_null_records:,} rows)")
        print(f"  Records {last_range_start:,}-{self.total_records-1:,}: Has backfill data ({self.total_records - last_range_start:,} rows)")
        print(f"Total records with data: {records_with_data:,}")
        print(f"Total records with NULL: {expected_null_records:,}")

        # Create DataFrame with backfill data (skip middle range)
        # Use pandas/pyarrow to write parquet directly (avoid Spark classloader issues)
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        data = {
            "pk": [],
            "new_field1": [],
            "new_field2": []
        }

        # First 30%
        for i in range(first_range_end):
            data["pk"].append(i)
            data["new_field1"].append(f"api_new_value_{i}")
            data["new_field2"].append(i * 2)

        # Last 30%
        for i in range(last_range_start, self.total_records):
            data["pk"].append(i)
            data["new_field1"].append(f"api_new_value_{i}")
            data["new_field2"].append(i * 2)

        # Create pandas DataFrame and save to parquet using pyarrow
        pdf = pd.DataFrame(data)
        temp_dir = tempfile.gettempdir()
        parquet_path = os.path.join(temp_dir, f"new_field_data_{int(time.time() * 1000)}.parquet")

        # Convert to pyarrow Table and write as parquet (no compression to avoid snappy dependency)
        table = pa.Table.from_pandas(pdf)
        pq.write_table(table, parquet_path, compression='none')

        print(f"\n✓ Backfill data saved to: {parquet_path}")
        print(f"  Columns: pk, new_field1, new_field2")
        print(f"  Rows: {len(pdf):,}")

        # Show first 10 rows using pandas
        print(pdf.head(10).to_string(index=False))

        return parquet_path, records_with_data, expected_null_records

    def create_snapshot(self):
        """Create snapshot and return S3 location path"""
        print("\n=== Creating Snapshot ===")

        print(f"Creating snapshot: {self.snapshot_name}")
        self.client.create_snapshot(
            self.collection_name,
            self.snapshot_name,
            "add field backfill snapshot"
        )

        # Get snapshot path from S3 location
        snapshot_info = self.client.describe_snapshot(self.snapshot_name)
        snapshot_path = snapshot_info['s3_location']

        print(f"✓ Snapshot created at: {snapshot_path}")

        # Format as s3a:// path for Spark/Hadoop
        s3a_path = f"s3a://{self.s3_bucket}/{snapshot_path}"
        print(f"  S3A path: {s3a_path}")

        return s3a_path

    def run_backfill(self, parquet_path, snapshot_path):
        """Execute backfill operation via Scala API"""
        print("\n=== Executing Backfill Operation ===")

        # Get Scala MilvusBackfill object via Py4J
        scala_package = "com.zilliz.spark.connector.operations.backfill"

        try:
            # Access Scala singleton object
            MilvusBackfill = self.spark._jvm.com.zilliz.spark.connector.operations.backfill.MilvusBackfill
            BackfillConfig = self.spark._jvm.com.zilliz.spark.connector.operations.backfill.BackfillConfig

            # Get Scala None for optional parameters (instead of Python None which becomes Java null)
            # Access Scala's None$ singleton object through Py4J
            scala_none = self.spark._jvm.scala.Option.empty()

            # Create BackfillConfig
            config = BackfillConfig(
                self.milvus_uri,
                self.milvus_token,
                "default",  # databaseName
                self.collection_name,
                scala_none,  # partitionName (Scala Option[String])
                self.s3_endpoint,
                self.s3_bucket,
                self.s3_access_key,
                self.s3_secret_key,
                False,  # s3UseSSL
                "files",  # s3RootPath
                "us-east-1",  # s3Region
                512,  # batchSize
                scala_none  # customOutputPath (Scala Option[String])
            )

            print(f"Backfill configuration:")
            print(f"  Collection: {self.collection_name}")
            print(f"  Backfill data: {parquet_path}")
            print(f"  Snapshot path: {snapshot_path}")
            print(f"  S3 endpoint: {self.s3_endpoint}")
            print(f"  Batch size: 512")

            # Execute backfill
            print("\nExecuting backfill...")
            result = MilvusBackfill.run(
                self.spark._jsparkSession,
                parquet_path,
                snapshot_path,
                config
            )

            # Check result (Scala Either type)
            if result.isRight():
                success = result.right().get()
                self._print_result(success)
                return True
            else:
                error = result.left().get()
                print(f"\n✗ Backfill failed: {error.message()}")
                return False

        except Exception as e:
            print(f"\n✗ Error calling Scala API: {e}")
            print("\nNote: Make sure the spark-milvus JAR is in the classpath")
            import traceback
            traceback.print_exc()
            return False

    def _print_result(self, success):
        """Print backfill result details"""
        print("\n=== Backfill Result ===")
        print(f"  Collection ID: {success.collectionId()}")
        print(f"  Partition ID: {success.partitionId()}")
        print(f"  Segments processed: {success.segmentResults().size()}")
        print(f"  New fields: {', '.join(success.newFieldNames())}")
        print(f"  Total execution time: {success.executionTimeMs()}ms")

        print("\n  Per-Segment Results:")
        total_rows = 0

        # Iterate over segment results (Scala Map)
        for entry in success.segmentResults().toSeq():
            segment_id = entry._1()
            seg_result = entry._2()

            print(f"  Segment {segment_id}:")
            print(f"    Rows: {seg_result.rowCount()}")
            print(f"    Execution time: {seg_result.executionTimeMs()}ms")
            print(f"    Output path: {seg_result.outputPath()}")

            total_rows += seg_result.rowCount()

        print(f"\n✓ Total rows processed: {total_rows:,}")

    def cleanup(self):
        """Cleanup resources"""
        print("\n=== Cleanup ===")

        if self.collection:
            print(f"Dropping collection: {self.collection_name}")
            utility.drop_collection(self.collection_name)

        if self.spark:
            print("Stopping Spark session...")
            self.spark.stop()

        connections.disconnect("default")
        print("✓ Cleanup complete")

    def run(self):
        """Main execution flow"""
        try:
            print("=" * 70)
            print("Milvus Backfill Demo - PySpark Edition")
            print("=" * 70)

            # Setup
            self.setup_spark()
            self.setup_milvus()

            # Prepare data
            self.insert_test_data()
            parquet_path, records_with_data, expected_null = self.create_backfill_data()

            # Create snapshot
            snapshot_path = self.create_snapshot()

            # Execute backfill
            success = self.run_backfill(parquet_path, snapshot_path)

            if success:
                print("\n" + "=" * 70)
                print("✓ Backfill demo completed successfully!")
                print("=" * 70)
            else:
                print("\n" + "=" * 70)
                print("✗ Backfill demo failed")
                print("=" * 70)

        except KeyboardInterrupt:
            print("\n\nDemo interrupted by user")
        except Exception as e:
            print(f"\n\n✗ Error during demo: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.cleanup()


def main():
    """Entry point"""
    demo = MilvusBackfillDemo()
    demo.run()


if __name__ == "__main__":
    main()
