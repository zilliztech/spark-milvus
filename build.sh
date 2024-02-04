mvn clean package -DskipTests

rm -rf output
mkdir output

cp target/spark-milvus-1.0.0-SNAPSHOT.jar output/