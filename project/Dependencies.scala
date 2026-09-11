import sbt._

/** Dependency coordinates, scopes and exclusions. Versions live in Versions. */
object Dependencies {
  lazy val munit = "org.scalameta" %% "munit" % Versions.munit
  lazy val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest
  lazy val grpcNetty =
    "io.grpc" % "grpc-netty-shaded" % Versions.grpcJava excludeAll ExclusionRule(
      organization = "org.slf4j"
    )
  lazy val scalapbRuntime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % Versions.scalapb
  lazy val scalapbRuntimeGrpc =
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % Versions.scalapb
  lazy val scalapbCompilerPlugin =
    "com.thesamet.scalapb" %% "compilerplugin" % Versions.scalapb
  // The root keeps its existing Spark 4.0 dependencies during migration.
  lazy val sparkCore =
    "org.apache.spark" %% "spark-core" % Versions.line("4.0").spark % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkSql =
    "org.apache.spark" %% "spark-sql" % Versions.line("4.0").spark % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkCatalyst =
    "org.apache.spark" %% "spark-catalyst" % Versions.line("4.0").spark % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  lazy val sparkMLlib =
    "org.apache.spark" %% "spark-mllib" % Versions.line("4.0").spark % "provided,test" excludeAll(
      ExclusionRule(organization = "org.apache.arrow")
    )
  // Spark supplies Hadoop, Parquet and Avro; tests also need them locally.
  lazy val parquetHadoop =
    "org.apache.parquet" % "parquet-hadoop" % Versions.parquet % "provided,test"
  // Spark does not ship parquet-avro, so retain it in the assembly.
  lazy val parquetAvro =
    "org.apache.parquet" % "parquet-avro" % Versions.parquetAvro
  lazy val avro =
    "org.apache.avro" % "avro" % Versions.avro % "provided,test"
  lazy val hadoopCommon =
    "org.apache.hadoop" % "hadoop-common" % Versions.hadoop % "provided,test" exclude ("javax.activation", "activation")
  lazy val hadoopMapreduceClientCore =
    "org.apache.hadoop" % "hadoop-mapreduce-client-core" % Versions.hadoop
  lazy val hadoopAws =
    "org.apache.hadoop" % "hadoop-aws" % Versions.hadoop % "provided,test" exclude("software.amazon.awssdk", "bundle")
  lazy val hadoopAliyun =
    "org.apache.hadoop" % "hadoop-aliyun" % Versions.hadoop % "provided,test"
  lazy val awsSdkS3 =
    "software.amazon.awssdk" % "s3" % Versions.awsSdkV2
  lazy val awsSdkS3Transfer =
    "software.amazon.awssdk" % "s3-transfer-manager" % Versions.awsSdkV2
  lazy val awsSdkCore =
    "com.amazonaws" % "aws-java-sdk-core" % Versions.awsSdkV1Core
  lazy val jacksonScala =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson
  lazy val jacksonDatabind =
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson

  // The root's Arrow baseline is separate from the Spark-line matrix.
  lazy val arrowFormat = "org.apache.arrow" % "arrow-format" % Versions.legacyRootArrow
  lazy val arrowVector = "org.apache.arrow" % "arrow-vector" % Versions.legacyRootArrow
  lazy val arrowMemoryCore = "org.apache.arrow" % "arrow-memory-core" % Versions.legacyRootArrow
  lazy val arrowMemoryNetty = "org.apache.arrow" % "arrow-memory-netty" % Versions.legacyRootArrow
  lazy val arrowCData = "org.apache.arrow" % "arrow-c-data" % Versions.legacyRootArrow

  /** Spark supplies these modules; Arrow is selected explicitly below. */
  def sparkDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("spark-core", "spark-sql", "spark-catalyst").map { m =>
      ("org.apache.spark" %% m % l.spark % "provided")
        .excludeAll(ExclusionRule(organization = "org.apache.arrow"))
    }

  /** The Arrow implementation for a Spark line. Core uses provided APIs. */
  def arrowDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("arrow-vector", "arrow-memory-core", "arrow-c-data", "arrow-memory-netty")
      .map(m => "org.apache.arrow" % m % l.arrow)

  /** Dependencies still needed by the migrated connector and app sources. */
  def legacyDeps(l: Versions.SparkLine): Seq[ModuleID] = Seq(
    ("org.apache.spark" %% "spark-mllib" % l.spark % "provided,test")
      .excludeAll(ExclusionRule(organization = "org.apache.arrow")),
    parquetHadoop,
    parquetAvro,
    avro,
    hadoopCommon,
    hadoopAws,
    hadoopAliyun,
    awsSdkS3,
    awsSdkS3Transfer,
    awsSdkCore,
    jacksonScala,
    jacksonDatabind,
    grpcNetty,
    scalapbRuntimeGrpc,
    munit % Test,
    hadoopMapreduceClientCore % Test
  )

  /** Preserve the root assembly's dependency set while modules take over. */
  lazy val legacyRootDeps: Seq[ModuleID] = Seq(
    munit % Test,
    scalaTest % Test,
    grpcNetty,
    scalapbRuntime % "protobuf",
    scalapbRuntimeGrpc,
    scalapbCompilerPlugin,
    sparkCore,
    sparkSql,
    sparkCatalyst,
    sparkMLlib,
    parquetHadoop,
    parquetAvro,
    avro,
    hadoopCommon,
    hadoopAws,
    hadoopAliyun,
    awsSdkS3,
    awsSdkS3Transfer,
    awsSdkCore,
    jacksonScala,
    jacksonDatabind,
    arrowFormat,
    arrowVector,
    arrowMemoryCore,
    arrowMemoryNetty,
    arrowCData
  )
}
