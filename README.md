# Spark Milvus Connector

## Introduction
The Spark Milvus Connector provides seamless integration between Apache Spark and Milvus, enabling users to leverage Spark's processing capabilities alongside Milvus's vector data storage and query functionalities. With this integration, numerous interesting applications become feasible, including:
- Transfer and integrate data between Milvus and various storage systems or databases
- Processing and analysing the data in Milvus
- Utilize Spark MLlib and other AI libraries for efficient vector processing operations.

## Development

<b>Prerequisites</b>

- Java 8 or higher
- Apache Maven

<b>Clone & Build</b>

```scala
git clone https://github.com/zilliztech/spark-milvus.git
cd spark-milvus 
mvn clean package -DskipTests
```
After the packaging, a new spark-milvus-1.0.0-SNAPSHOT.jar is generated in spark-milvus/target/ directory.


## Usages


<b>Enable Spark Milvus Connector in Spark environment</b>

To enable spark-milvus connector in Spark, add jar in Spark start command or add it in class path.

```shell
# pyspark
./bin/pyspark --jars spark-milvus-1.0.0-SNAPSHOT.jar
# spark-shell
./bin/spark-shell --jars spark-milvus-1.0.0-SNAPSHOT.jar
# spark-submit
./bin/spark-submit --jars spark-milvus-1.0.0-SNAPSHOT.jar ......
```

<b>Add spark-milvus-connector dependency in a maven project</b>

```scala
<dependency>
    <groupId>zilliztech</groupId>
    <artifactId>spark-milvus</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Quick Start
In this demo, we create a sample dataframe and write it to milvus through spark milvus connector. A collection will be created automatically based on the schema and some options.

python
```python
from pyspark.sql import SparkSession

columns = ["id", "text", "vec"]
data = [(1, "a", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (2, "b", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (3, "c", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (4, "d", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0])]
sample_df = spark.sparkContext.parallelize(data).toDF(columns)
sample_df.write \
    .mode("append") \
    .option("milvus.host", "localhost") \
    .option("milvus.port", "19530") \
    .option("milvus.collection.name", "hello_spark_milvus") \
    .option("milvus.collection.vectorField", "vec") \
    .option("milvus.collection.vectorDim", "5") \
    .option("milvus.collection.primaryKeyField", "id") \
    .format("milvus") \
    .save()
```

Scala

```scala
import org.apache.spark.sql.{SaveMode, SparkSession}

object Hello extends App {

  val spark = SparkSession.builder().master("local[*]")
    .appName("HelloSparkMilvus")
    .getOrCreate()

  import spark.implicits._

  // Create DataFrame
  val sampleDF = Seq(
    (1, "a", Seq(1.0,2.0,3.0,4.0,5.0)),
    (2, "b", Seq(1.0,2.0,3.0,4.0,5.0)),
    (3, "c", Seq(1.0,2.0,3.0,4.0,5.0)),
    (4, "d", Seq(1.0,2.0,3.0,4.0,5.0))
  ).toDF("id", "text", "vec")

  // set milvus options
  val milvusOptions = Map(
      "milvus.host" -> "localhost",
      "milvus.port" -> "19530",
      "milvus.collection.name" -> "hello_spark_milvus",
      "milvus.collection.vectorField" -> "vec",
      "milvus.collection.vectorDim" -> "5",
      "milvus.collection.primaryKeyField" -> "id"
    )
    
  sampleDF.write.format("milvus")
    .options(milvusOptions)
    .mode(SaveMode.Append)
    .save()
}
```

### More demos

[scala](examples/src/main/scala/)
- [InsertDemo](examples/src/main/scala/InsertDemo.scala)
- [ReadDemo](examples/src/main/scala/ReadDemo.scala)
- [ReadMilvusBinlogDemo](examples/src/main/scala/ReadMilvusBinlogDemo.scala)
- [Mysql2MilvusDemo](examples/src/main/scala/Mysql2MilvusDemo.scala)
- [BulkInsertDemo](examples/src/main/scala/BulkInsertDemo.scala)

[python](examples/py/)
- [QuickStart](examples/py/quickstart.py)

[Databricks notebooks](examples/databricks-notebook)


## Key Features & Concepts

### Milvus Options
Milvus Options are the configs to create Milvus connections and manage all other Milvus behaviors.
Each spark-milvus job may need several(not all) of them.
See [MilvusOptions](src/main/scala/zilliztech/spark/milvus/MilvusOptions.scala) for detail

### Milvus Data format

<b>milvus</b>

"milvus" is a new data format supporting seemlessly write Spark DataFrame data into Milvus Collections. It is achieved by batch calls to the Insert API of Milvus SDK. A new collection will be created based on the schema of the dataframe if it doesn't exist in Milvus. However, the automatically created collection can't support all features of collection schema. It is recommended to create a collection via SDK first and do the writing with spark-milvus. Please refer to demo

```scala
val milvusOptions = Map(
  MILVUS_HOST -> host,
  MILVUS_PORT -> port.toString,
  MILVUS_COLLECTION_NAME -> collectionName,
)
val df = spark.write
  .format("milvus")
  .options(milvusOptions)
  .mode(SaveMode.Append)
  .save()
```

<b>milvusbinlog</b>

New data format "milvusbinlog" is for reading Milvus built-in binlog data.
Binlog is Milvus's specific data storage format based on parquet.
Unfortunately it can't be read by a regular parquet library.
We implement this new dataformat enabling Spark to read it.
It is not suggested to use "milvusbinlog" directly unless you are familiar to the milvus storage hierarchy and need to dig into the data.
Instead, it is encapsulated within the MilvusUtils functions for a more convenient and recommended approach.
MilvusUtils is introduced in the next section.

```scala
val df = spark.read
  .format("milvusbinlog")
  .load(path)
  .withColumnRenamed("val", "embedding")
```

<b>mjson</b>

Milvus provides Bulkinsert function(https://milvus.io/docs/bulk_insert.md) which has a better writing performance than Insert. However, the json format required by Milvus is different from Spark's output.
To resolve this, we introduce "mjson" data format to generate data that meets Milvus requirement.

```scala
val df = spark.write
  .format("mjson")
  .mode(SaveMode.Overwrite)
  .save(path)
```

### MilvusUtils

MilvusUtils contains several util functions that make codes easier.
Currently it is only supported in Scala.
Python developers can write your own utils.
How to use them is shown in Advanced Usage.

<B>MilvusUtils.readMilvusCollection</B>

MilvusUtils.readMilvusCollection wrap needed SDK callings, milvusbinlog reading and some union/join logic to load a whole collection into a Spark dataframe.

```scala
val collectionDF = MilvusUtils.readMilvusCollection(spark, milvusOptions)
```

<b>MilvusUtils.bulkInsertFromSpark</b>

MilvusUtils.bulkInsertFromSpark provides a quick way to import Spark output files into Milvus via bullkinserts.

```scala
df.write.format("parquet").save(outputPath)
MilvusUtils.bulkInsertFromSpark(spark, milvusOptions, outputPath, "parquet")
```

## Contributing
Contributions to spark-milvus are welcome from everyone.
Feel free to create an issue if you meet any problem in using spark-milvus.
Go to [Milvus discord](https://discord.com/channels/1160323594396635310) to discuss, raise questions, share thoughts with other users and developers.

## License
spark-milvus is licensed under the Apache License, Version 2.0.