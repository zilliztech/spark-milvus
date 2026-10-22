import sbt._

/** Dependency coordinates, scopes and exclusions. Versions live in Versions. */
object Dependencies {
  lazy val munit = "org.scalameta" %% "munit" % Versions.munit
  lazy val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest
  lazy val scalapbRuntime =
    "com.thesamet.scalapb" %% "scalapb-runtime" % Versions.scalapb
  // Spark supplies Hadoop, Parquet and Avro; tests also need them locally.
  lazy val parquetHadoop =
    "org.apache.parquet" % "parquet-hadoop" % Versions.parquet % "provided,test"
  // Spark does not ship parquet-avro, so retain it in the assembly.
  lazy val parquetAvro =
    "org.apache.parquet" % "parquet-avro" % Versions.parquetAvro
  lazy val avro =
    "org.apache.avro" % "avro" % Versions.avro % "provided,test"
  lazy val hadoopCommon = hadoopCommonAt(Versions.hadoop)
  def hadoopCommonAt(version: String): ModuleID =
    "org.apache.hadoop" % "hadoop-common" % version % "provided,test" exclude ("javax.activation", "activation")
  lazy val hadoopAws =
    "org.apache.hadoop" % "hadoop-aws" % Versions.hadoop % "provided,test" exclude ("software.amazon.awssdk", "bundle")
  lazy val awsSdkS3 =
    "software.amazon.awssdk" % "s3" % Versions.awsSdkV2
  // Required by WebIdentityTokenFileCredentialsProvider: it looks up
  // WebIdentityTokenCredentialsProviderFactory, whose implementation ships in
  // this module. Without it the provider constructs, then throws when
  // resolving, and a provider chain silently falls through to the node role.
  lazy val awsSdkSts =
    "software.amazon.awssdk" % "sts" % Versions.awsSdkV2
  lazy val awsSdkS3Transfer =
    "software.amazon.awssdk" % "s3-transfer-manager" % Versions.awsSdkV2
  lazy val awsSdkCore =
    "com.amazonaws" % "aws-java-sdk-core" % Versions.awsSdkV1Core
  lazy val jacksonScala =
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson
  lazy val jacksonDatabind =
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson

  /** xxh3 for the primary-key bloom filter (core.stats). Ships in the assembly:
    * Spark does not carry it.
    */
  lazy val zeroAllocationHashing =
    "net.openhft" % "zero-allocation-hashing" % Versions.zeroAllocationHashing

  /** Spark supplies these modules; Arrow is selected explicitly below. */
  def sparkDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq("spark-core", "spark-sql", "spark-catalyst").map { m =>
      ("org.apache.spark" %% m % l.spark % "provided")
        .excludeAll(ExclusionRule(organization = "org.apache.arrow"))
    }

  /** The Arrow implementation for a Spark line. Core uses provided APIs. */
  def arrowDeps(l: Versions.SparkLine): Seq[ModuleID] =
    Seq(
      "arrow-vector",
      "arrow-memory-core",
      "arrow-c-data",
      "arrow-memory-netty"
    )
      .map(m => "org.apache.arrow" % m % l.arrow)

  /** What a Spark line's classpath resolves to no matter who asks for them: the
    * Arrow, Netty and Hadoop versions its Spark distribution ships. Netty is
    * the one that bites: hadoop-common 3.4.1 and the AWS SDK both raise it to
    * 4.1.118, where PoolArena no longer extends SizeClasses, and Arrow 12's
    * allocation manager fails on its first buffer with NoSuchFieldError (work
    * item 19). Pinning the whole Netty family keeps its modules on one version.
    */
  def lineOverrides(l: Versions.SparkLine): Seq[ModuleID] =
    Seq(
      "arrow-vector",
      "arrow-memory-core",
      "arrow-memory-netty",
      "arrow-c-data",
      "arrow-format"
    ).map(m => "org.apache.arrow" % m % l.arrow) ++
      Seq(
        "netty-buffer",
        "netty-common",
        "netty-handler",
        "netty-handler-proxy",
        "netty-codec",
        "netty-codec-http",
        "netty-codec-http2",
        "netty-codec-socks",
        "netty-resolver",
        "netty-transport",
        "netty-transport-native-unix-common",
        "netty-transport-classes-epoll",
        "netty-transport-native-epoll",
        "netty-transport-classes-kqueue",
        "netty-transport-native-kqueue"
      ).map(m => "io.netty" % m % l.netty) ++
      Seq(
        "hadoop-common",
        "hadoop-auth",
        "hadoop-annotations",
        "hadoop-client-api",
        "hadoop-client-runtime"
      ).map(m => "org.apache.hadoop" % m % l.hadoop)

  /** Dependencies needed by snapshot reads, search and storage credentials. */
  def readerDeps(l: Versions.SparkLine): Seq[ModuleID] = Seq(
    ("org.apache.spark" %% "spark-mllib" % l.spark % "provided,test")
      .excludeAll(ExclusionRule(organization = "org.apache.arrow")),
    hadoopCommonAt(l.hadoop),
    // Shipped, not compiled against: these supply the credential provider
    // classes that fs.s3a.aws.credentials.provider names by string. Removing
    // one fails when a FileSystem is built, not at compile time.
    awsSdkS3,
    awsSdkS3Transfer,
    awsSdkSts,
    awsSdkCore,
    parquetAvro,
    jacksonScala,
    jacksonDatabind,
    munit % Test
  )

}
