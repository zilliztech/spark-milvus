#!/usr/bin/env python3
"""
Helper script to create Milvus snapshot and return S3 location.
Used by Scala tests that need snapshot functionality before it's added to proto.

Usage:
    python scripts/create_snapshot.py <collection_name> <snapshot_name> [description]

Returns:
    Prints JSON with snapshot info including s3_location
"""

import sys
import json
from pymilvus import MilvusClient

def main():
    if len(sys.argv) < 3:
        print(json.dumps({
            "error": "Usage: create_snapshot.py <collection_name> <snapshot_name> [description]"
        }))
        sys.exit(1)

    collection_name = sys.argv[1]
    snapshot_name = sys.argv[2]
    description = sys.argv[3] if len(sys.argv) > 3 else ""

    # Connect to Milvus
    client = MilvusClient(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    try:
        # Create snapshot
        client.create_snapshot(
            collection_name=collection_name,
            snapshot_name=snapshot_name,
            description=description
        )

        # Get snapshot info
        info = client.describe_snapshot(snapshot_name=snapshot_name)

        # Output as JSON
        result = {
            "name": info["name"],
            "description": info["description"],
            "collection_name": info["collection_name"],
            "partition_names": info["partition_names"],
            "create_ts": info["create_ts"],
            "s3_location": info["s3_location"]
        }
        print(json.dumps(result))

    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()
