import sbt._

object Dependencies {
  // Version constants
  lazy val scalapbVersion = "0.11.3"
  lazy val sparkVersion = "4.0.0"
  lazy val grpcJavaVersion = "1.37.0"
  // lazy val sparkVersion = "3.3.2"
  lazy val parquetVersion = "1.13.1"
  lazy val hadoopVersion =
    "3.4.1" // can't be changed https://github.com/apache/spark/blob/v3.5.3/pom.xml
  lazy val jacksonVersion = "2.17.3"

  lazy val munit = "org.scalameta" %% "munit" % "0.7.29"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
  lazy val grpcNetty =
    "io.grpc" % "grpc-netty-shaded" % grpcJavaVersion excludeAll ExclusionRule(
      organization = "org.slf4j"
    )
  lazy val scalapbRuntime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion
  lazy val scalapbRuntimeGrpc =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion
  lazy val scalapbCompilerPlugin =
    "com.thesamet.scalapb" %% "compilerplugin" % scalapbVersion
  lazy val sparkCore =
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkSql =
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkCatalyst =
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkMLlib =
    "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val parquetHadoop =
    "org.apache.parquet" % "parquet-hadoop" % parquetVersion
  lazy val hadoopCommon =
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion exclude ("javax.activation", "activation")
  lazy val hadoopAws =
    "org.apache.hadoop" % "hadoop-aws" % hadoopVersion exclude("software.amazon.awssdk", "bundle")
  lazy val awsSdkS3 =
    "software.amazon.awssdk" % "s3" % "2.30.38" // doc: https://javadoc.io/doc/software.amazon.awssdk/s3/2.30.38/index.html
  lazy val awsSdkS3Transfer = 
    "software.amazon.awssdk" % "s3-transfer-manager" % "2.30.38"
  lazy val awsSdkCore =
    "com.amazonaws" % "aws-java-sdk-core" % "1.12.780"
  lazy val jacksonScala =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
  lazy val jacksonDatabind =
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion

  // Arrow dependencies for milvus-storage JNI
  lazy val arrowVersion = "17.0.0"
  lazy val arrowFormat = "org.apache.arrow" % "arrow-format" % arrowVersion
  lazy val arrowVector = "org.apache.arrow" % "arrow-vector" % arrowVersion
  lazy val arrowMemoryCore = "org.apache.arrow" % "arrow-memory-core" % arrowVersion
  lazy val arrowMemoryNetty = "org.apache.arrow" % "arrow-memory-netty" % arrowVersion
  lazy val arrowCData = "org.apache.arrow" % "arrow-c-data" % arrowVersion
}
