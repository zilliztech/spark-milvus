from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Milvus Integration") \
    .master("local[*]") \
    .getOrCreate()
columns = ["id", "text", "vec"]
data = [(1, "a", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (2, "b", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (3, "c", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0]),
    (4, "d", [1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0])]
sample_df = spark.sparkContext.parallelize(data).toDF(columns)

# .option("milvus.uri", "https://localhost:19530") \
# .option("milvus.token", "root:Milvus") \

# tls enabled
# .option("milvus.secure", "true") \
# .option("milvus.secure.serverName", "servername") \
# .option("milvus.secure.serverPemPath", "path/to/server.pem") \

# mTLS enabled
# .option("milvus.secure", "true") \
# .option("milvus.secure.serverName", "hostname") \
# .option("milvus.secure.caCert", "path/ca.pem") \
# .option("milvus.secure.clientCert", "/path/client.pem") \  
# .option("milvus.secure.clientKey", "/path/client.key") \ 

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