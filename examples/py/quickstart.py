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
    .option("milvus.collection.vectorDim", "8") \
    .option("milvus.collection.primaryKeyField", "id") \
    .format("milvus") \
    .save()