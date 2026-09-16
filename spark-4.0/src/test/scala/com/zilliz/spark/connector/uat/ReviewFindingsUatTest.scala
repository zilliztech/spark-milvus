package com.zilliz.spark.connector.uat

import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.snapshot.{SnapshotCatalog, V2SegmentResolver}
import com.zilliz.milvus.storage.snapshot.json.{
  ManifestItemJson,
  SegmentListJson
}
import com.zilliz.milvus.storage.stats.PrimaryKeyStats
import com.zilliz.milvus.storage.write.commit.{Committer, JobManifest}
import com.zilliz.milvus.storage.write.exec.StagingLayout
import com.zilliz.spark.connector.options.{
  HadoopStorageKeys,
  MilvusOption,
  StorageOptions
}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType => MilvusDataType,
  FieldSchema
}

/** Spark jobs that drive the public entry points the way the findings of the
  * 2026-09-16 review of 749178e were triggered, against the UAT instance. Each
  * test names the findings it exercises. Everything written goes under a fresh
  * prefix below `MILVUS_UAT_WRITE_PREFIX` and is deleted at the end; a
  * collection a test creates is dropped.
  *
  * Environment: `MILVUS_JNI_S3_BUCKET`, `MILVUS_JNI_S3_REGION`,
  * `MILVUS_JNI_S3_ROOT_PATH`, `AWS_*` (the instance role's temporary
  * credentials, reached through the default provider chain),
  * `MILVUS_UAT_TYPES_SNAPSHOT_PATH`, `MILVUS_UAT_V3_DELETED_SNAPSHOT`,
  * `MILVUS_UAT_URI`, `MILVUS_UAT_TOKEN`, `MILVUS_UAT_PARTS_COLLECTION`,
  * `MILVUS_UAT_WRITE_PREFIX` (default spark-uat-write).
  */
class ReviewFindingsUatTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)
  private def need(n: String): String = env(n).getOrElse(cancel(s"set $n"))

  private lazy val bucket = need("MILVUS_JNI_S3_BUCKET")
  private lazy val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
  private lazy val endpoint = s"s3.$region.amazonaws.com"
  private lazy val reviewRoot =
    s"${env("MILVUS_UAT_WRITE_PREFIX").getOrElse("spark-uat-write")}/review-${System.currentTimeMillis()}"

  /** No keys, no fs.use_iam: the IAM fallback has to supply the identity, on
    * the read and on the write (#10). Spark copies the driver's AWS_*
    * variables, the instance role's temporary credential, into the session's
    * s3a keys; those go to the native default chain, which reads the same
    * variables with their session token.
    */
  private def storage(root: String = reviewRoot): Map[String, String] = Map(
    StorageProperties.BucketName -> bucket,
    StorageProperties.Address -> endpoint,
    StorageProperties.Region -> region,
    StorageProperties.CloudProvider -> "aws",
    StorageProperties.UseSSL -> "true",
    StorageProperties.RootPath -> root
  )

  private var sparkSession: SparkSession = null
  private def spark: SparkSession = {
    if (sparkSession == null) {
      sparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("review-findings-uat")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    }
    sparkSession
  }

  private def store(): ObjectStore =
    HadoopStorageKeys.storeFrom(
      storage() + (StorageProperties.UseIam -> "true")
    )

  override protected def afterAll(): Unit = {
    if (env("MILVUS_JNI_S3_BUCKET").isDefined && sparkSession != null) {
      val s = store()
      try {
        val files =
          s.list(reviewRoot, recursive = true).filterNot(_.isDirectory)
        files.foreach(f => s.delete(f.path))
        info(s"deleted ${files.size} files under $reviewRoot")
      } finally s.close()
    }
    if (sparkSession != null) sparkSession.stop()
  }

  // ------------------------------------------------------------ helpers

  private def schemaOptions(
      schema: CollectionSchema,
      root: String
  ): Map[String, String] = storage(root) ++ Map(
    MilvusOption.SnapshotMode -> "true",
    MilvusOption.SnapshotSchemaBytes -> Base64.getEncoder.encodeToString(
      schema.toByteArray
    ),
    MilvusOption.SnapshotCollectionId -> "1",
    MilvusOption.SnapshotPartitionIds -> "0",
    MilvusOption.MilvusCollectionName -> schema.name,
    MilvusOption.MilvusInsertMaxBatchSize -> "1000"
  )

  /** The one job committed under `root`, and its manifest. */
  private def committed(root: String): (StagingLayout, JobManifest) = {
    val s = store()
    try {
      val jobs = s
        .list(s"$root/staging", recursive = false)
        .filter(_.isDirectory)
        .map(d => StagingLayout(root, d.path.stripSuffix("/").split("/").last))
        .filter(l => s.exists(l.marker))
      withClue(s"committed jobs under $root/staging: ")(jobs should have size 1)
      val layout = jobs.head
      val manifest = JobManifest
        .fromJson(
          new String(s.readAll(layout.manifest), StandardCharsets.UTF_8)
        )
        .fold(e => throw e, identity)
      (layout, manifest)
    } finally s.close()
  }

  private def readBack(
      schema: CollectionSchema,
      manifest: JobManifest,
      columnar: Boolean
  ): DataFrame = {
    val manifests = SegmentListJson.encodeManifestItems(
      manifest.segments.map(_.basePath).zipWithIndex.map { case (path, i) =>
        ManifestItemJson(i + 1L, s"""{"ver":-1,"base_path":"$path"}""")
      }
    )
    spark.read
      .format("milvus")
      .options(
        schemaOptions(schema, reviewRoot) ++ Map(
          MilvusOption.SnapshotManifests -> manifests,
          MilvusOption.ReadColumnar -> columnar.toString
        )
      )
      .load()
  }

  private def plain(v: Any): Any = v match {
    case null                       => null
    case b: Array[Byte]             => b.toSeq
    case s: scala.collection.Seq[_] => s.map(plain).toList
    case other                      => other
  }

  private def byId(df: DataFrame, idColumn: String): Map[Any, Seq[Any]] =
    df.collect()
      .map(r =>
        r.getAs[Any](idColumn) -> df.columns.toSeq.map(c =>
          plain(r.getAs[Any](c))
        )
      )
      .toMap

  private def client() = com.zilliz.milvus.client.api.MilvusClient(
    MilvusOption(
      Map(MilvusOption.MilvusUri -> need("MILVUS_UAT_URI")) ++
        env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
    ).connectionParams
  )

  // ------------------------------------------------------------ #01 #04 #10 JSON

  test(
    "#01 #04 #10: the all-types collection written back through df.write reads back value for value"
  ) {
    val snapshotPath = need("MILVUS_UAT_TYPES_SNAPSHOT_PATH")
    val root = s"$reviewRoot/types"
    val options = storage(root) + (MilvusOption.SnapshotPath -> snapshotPath)
    val source = spark.read.format("milvus").options(options).load()
    source.schema("i8").dataType shouldBe ByteType
    source.schema("arr").dataType shouldBe ArrayType(LongType)
    source.schema("j").dataType shouldBe StringType
    // No keys and no fs.use_iam in `options`: before the fix every task failed
    // with "fs.access_key_id must be set" (#10); arr made every task fail with
    // "Lists have one child Field" (#04); i8 was written as null (#01).
    source
      .repartition(2)
      .write
      .format("milvus")
      .mode("append")
      .options(options)
      .save()

    val (_, manifest) = committed(root)
    manifest.rowCount shouldBe 100L
    val collection = {
      val s = store()
      try
        new SnapshotCatalog(
          s,
          bucket,
          V2SegmentResolver.Skipped,
          endpoint = endpoint
        ).read(snapshotPath).schema
      finally s.close()
    }
    val expected = byId(source, "id")
    Seq(true, false).foreach { columnar =>
      withClue(s"columnar=$columnar: ") {
        val back = readBack(collection, manifest, columnar)
          .select(source.columns.map(col): _*)
        val got = byId(back, "id")
        got.keySet shouldBe expected.keySet
        expected.foreach { case (id, row) => got(id) shouldBe row }
        back.filter(col("i8").isNull).count() shouldBe 0L
      }
    }
  }

  // ------------------------------------------------------------ #04 #05

  private val arraysSchema = CollectionSchema(
    name = "review_arrays",
    fields = Seq(
      FieldSchema(
        fieldID = 100,
        name = "id",
        dataType = MilvusDataType.Int64,
        isPrimaryKey = true
      )
    ) ++ Seq(
      (101L, "a_f", MilvusDataType.Float),
      (102L, "a_i8", MilvusDataType.Int8),
      (103L, "a_i16", MilvusDataType.Int16),
      (104L, "a_b", MilvusDataType.Bool),
      (105L, "a_d", MilvusDataType.Double),
      (106L, "a_s", MilvusDataType.VarChar)
    ).map { case (id, name, element) =>
      FieldSchema(
        fieldID = id,
        name = name,
        dataType = MilvusDataType.Array,
        elementType = element,
        nullable = true,
        typeParams = Seq(
          KeyValuePair("max_capacity", "8"),
          KeyValuePair("max_length", "16")
        )
      )
    }
  )

  test(
    "#04 #05: arrays of every element type are written and read back on both outlets"
  ) {
    need("MILVUS_JNI_S3_BUCKET")
    val root = s"$reviewRoot/arrays"
    val sparkSchema = StructType(
      Seq(
        StructField("id", LongType),
        StructField("a_f", ArrayType(FloatType)),
        StructField("a_i8", ArrayType(ShortType)),
        StructField("a_i16", ArrayType(ShortType)),
        StructField("a_b", ArrayType(BooleanType)),
        StructField("a_d", ArrayType(DoubleType)),
        StructField("a_s", ArrayType(StringType))
      )
    )
    val rows = (0L until 60L).map { id =>
      if (id % 10 == 0) Row(id, null, null, null, null, null, null)
      else if (id % 10 == 1)
        Row(
          id,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty,
          Seq.empty
        )
      else
        Row(
          id,
          Seq(id * 0.5f, -id.toFloat),
          Seq((id % 128).toShort, (-(id % 128)).toShort),
          Seq((id * 100).toShort),
          Seq(id % 2 == 0, true),
          Seq(id * 0.25),
          Seq(s"s$id", "中")
        )
    }
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(rows, 2),
      sparkSchema
    )
    df.write
      .format("milvus")
      .mode("append")
      .options(schemaOptions(arraysSchema, root))
      .save()
    val (_, manifest) = committed(root)
    manifest.rowCount shouldBe 60L
    val expected = byId(df, "id")
    Seq(true, false).foreach { columnar =>
      withClue(s"columnar=$columnar: ") {
        // Before #05 the row path failed on a_f, a_i8 and a_i16 with
        // "Cannot decode binary-backed vector ... for Milvus type Array".
        val back = readBack(arraysSchema, manifest, columnar)
          .select(df.columns.map(col): _*)
        val got = byId(back, "id")
        got.keySet shouldBe expected.keySet
        expected.foreach { case (id, row) => got(id) shouldBe row }
      }
    }
  }

  // ------------------------------------------------------------ #12

  test("#12: a VarChar primary key's bounds follow Milvus's UTF-8 byte order") {
    need("MILVUS_JNI_S3_BUCKET")
    val root = s"$reviewRoot/varchar-pk"
    val schema = CollectionSchema(
      name = "review_varchar_pk",
      fields = Seq(
        FieldSchema(
          fieldID = 100,
          name = "pk",
          dataType = MilvusDataType.VarChar,
          isPrimaryKey = true,
          typeParams = Seq(KeyValuePair("max_length", "16"))
        ),
        FieldSchema(fieldID = 101, name = "n", dataType = MilvusDataType.Int64)
      )
    )
    val pua = new String(Character.toChars(0xe000))
    val emoji = new String(Character.toChars(0x1f600))
    val keys = Seq("row-0", "中文", "Ａ", pua, emoji)
    val df = spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          keys.zipWithIndex.map { case (k, i) => Row(k, i.toLong) },
          1
        ),
        StructType(
          Seq(StructField("pk", StringType), StructField("n", LongType))
        )
      )
    df.write
      .format("milvus")
      .mode("append")
      .options(schemaOptions(schema, root))
      .save()
    committed(root)
    val s = store()
    try {
      val statsFiles = s
        .list(root, recursive = true)
        .filter(f =>
          !f.isDirectory && f.path.contains("_stats/bloom_filter.100/")
        )
      statsFiles should have size 1
      val stats = PrimaryKeyStats.fromJson(
        new String(s.readAll(statsFiles.head.path), StandardCharsets.UTF_8)
      )
      info(s"minPk=${stats.minPk} maxPk=${stats.maxPk}")
      // UTF-16 order made "Ａ" the maximum; Milvus's order makes it the emoji.
      stats.minPk shouldBe "row-0"
      stats.maxPk shouldBe emoji
      keys.foreach(k => stats.mightContainString(k) shouldBe true)
    } finally s.close()
  }

  // ------------------------------------------------------------ #09

  test("#09: a Hadoop-only endpoint on port 443 is reached over TLS") {
    val snapshotPath = need("MILVUS_UAT_V3_DELETED_SNAPSHOT")
    val hadoop = spark.sparkContext.hadoopConfiguration
    hadoop.set("fs.s3a.endpoint", s"$endpoint:443")
    hadoop.set("fs.s3a.endpoint.region", region)
    try {
      // No fs.address and no fs.use_ssl: both come from the Hadoop keys. Before
      // the fix the native layer spoke plain HTTP to port 443 and failed.
      val count = spark.read
        .format("milvus")
        .option(MilvusOption.SnapshotPath, snapshotPath)
        .option(StorageProperties.BucketName, bucket)
        .option(StorageProperties.UseIam, "true")
        .load()
        .count()
      count shouldBe 2990L
    } finally {
      hadoop.unset("fs.s3a.endpoint")
      hadoop.unset("fs.s3a.endpoint.region")
    }
  }

  // ------------------------------------------------------------ #08

  test(
    "#08: with a session-wide AssumeRole, fs.use_iam keeps the role and static keys drop it"
  ) {
    val snapshotPath = need("MILVUS_UAT_V3_DELETED_SNAPSHOT")
    val fakeRole = "arn:aws:iam::000000000000:role/spark-milvus-review-08"
    val base = Map(
      MilvusOption.SnapshotPath -> snapshotPath,
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> endpoint,
      StorageProperties.Region -> region,
      StorageProperties.UseSSL -> "true"
    )
    def read(extra: Map[String, String]) =
      spark.read.format("milvus").options(base ++ extra).load().count()
    val iam = Map(StorageProperties.UseIam -> "true")
    val keys = Map(
      StorageProperties.AccessKeyId -> "AKIAREVIEW08",
      StorageProperties.AccessKeyValue -> "review-08"
    )
    read(iam) shouldBe 2990L

    val hadoop = spark.sparkContext.hadoopConfiguration
    hadoop.set(
      "fs.s3a.aws.credentials.provider",
      HadoopStorageKeys.S3AAssumedRoleProvider
    )
    hadoop.set("fs.s3a.assumed.role.arn", fakeRole)
    try {
      // The same read, with a role the instance identity cannot assume: it
      // fails, so the role reached the native layer.
      val e = intercept[Throwable](read(iam))
      info(s"fs.use_iam under the role: ${e.getMessage.take(300)}")
      // The UAT has no long-term keys, so the static-key half is checked on
      // the property bag this session resolves: before the fix it carried
      // the role as well, and the native layer prefers the role.
      def resolved(extra: Map[String, String]) = {
        val options = base ++ extra
        StorageOptions.storagePropertiesFor(
          StorageOptions.buildHadoopConfForOptions(options, ""),
          bucket,
          options
        )
      }
      resolved(iam).get(StorageProperties.RoleArn) shouldBe Some(fakeRole)
      resolved(keys).get(StorageProperties.RoleArn) shouldBe None
      resolved(keys).get(StorageProperties.AccessKeyId) shouldBe Some(
        "AKIAREVIEW08"
      )
    } finally {
      hadoop.unset("fs.s3a.aws.credentials.provider")
      hadoop.unset("fs.s3a.assumed.role.arn")
    }
  }

  // ------------------------------------------------------------ #06

  test(
    "#06: a user field named timestamp does not break a read that applies deletes"
  ) {
    need("MILVUS_JNI_S3_BUCKET")
    val name = s"spark_uat_review_ts_${System.currentTimeMillis()}"
    val c = client()
    try {
      import io.milvus.grpc.schema._
      val schema = c.createCollectionSchema(
        name = name,
        fields = Seq(
          c.createCollectionField(
            "id",
            isPrimary = true,
            dataType = DataType.Int64
          ),
          c.createCollectionField("timestamp", dataType = DataType.Int64),
          c.createCollectionField(
            "v",
            dataType = DataType.FloatVector,
            typeParams = Map("dim" -> "4")
          )
        )
      )
      c.createCollection(collectionName = name, schema = schema).get
      val ids = (0L until 200L)
      c.insert(
        collectionName = name,
        fieldsData = Seq(
          FieldData(
            `type` = DataType.Int64,
            fieldName = "id",
            field = FieldData.Field.Scalars(
              ScalarField(data =
                ScalarField.Data.LongData(LongArray(data = ids))
              )
            )
          ),
          FieldData(
            `type` = DataType.Int64,
            fieldName = "timestamp",
            field = FieldData.Field.Scalars(
              ScalarField(data =
                ScalarField.Data.LongData(LongArray(data = ids.map(_ * 7)))
              )
            )
          ),
          FieldData(
            `type` = DataType.FloatVector,
            fieldName = "v",
            field = FieldData.Field.Vectors(
              VectorField(
                dim = 4,
                data = VectorField.Data.FloatVector(
                  FloatArray(data =
                    ids.flatMap(i => (0 until 4).map(d => (i * 10 + d).toFloat))
                  )
                )
              )
            )
          )
        ),
        numRows = ids.size
      ).get
      c.flush(collectionNames = Seq(name)).get
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      c.delete[Int](
        collectionName = name,
        pkName = Some("id"),
        pks = 0 until 10
      ).get
      c.flush(collectionNames = Seq(name)).get
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val snapshot = c
        .createSnapshotForRead("", name, s"${name}_s", "review #06", 3600L)
        .get
      info(s"snapshot ${snapshot.name} at ${snapshot.s3Location}")
      Seq(true, false).foreach { columnar =>
        withClue(s"columnar=$columnar: ") {
          // Before the fix the native reader was asked for column 1, which the
          // schema it was handed did not have.
          val df = spark.read
            .format("milvus")
            .options(storage() + (StorageProperties.UseIam -> "true"))
            .option(MilvusOption.SnapshotPath, snapshot.s3Location)
            .option(MilvusOption.ReadColumnar, columnar.toString)
            .option(MilvusOption.MilvusExtraColumns, "_timestamp")
            .load()
          df.count() shouldBe 190L
          df.filter(col("id") < 10).count() shouldBe 0L
          df.filter(col("timestamp") =!= col("id") * 7).count() shouldBe 0L
          df.filter(col("_timestamp") <= 0).count() shouldBe 0L
        }
      }
      val clash = intercept[IllegalArgumentException](
        spark.read
          .format("milvus")
          .options(storage() + (StorageProperties.UseIam -> "true"))
          .option(MilvusOption.SnapshotPath, snapshot.s3Location)
          .option(MilvusOption.ReaderFieldIDs, "1,101")
          .load()
          .schema
      )
      clash.getMessage should include("differ only in case")
      c.dropSnapshot("", name, s"${name}_s")
    } finally {
      c.dropCollection(collectionName = name)
      c.close()
    }
  }

  // ------------------------------------------------------------ #07

  test(
    "#07: a broken older snapshot does not stop the latest from being read"
  ) {
    val collection = need("MILVUS_UAT_PARTS_COLLECTION")
    val instanceRoot = need("MILVUS_JNI_S3_ROOT_PATH")
    val c = client()
    val collectionId =
      try c.getCollectionInfo("", collection).get.collectionID
      finally c.close()
    val scratchRoot = s"$reviewRoot/catalog"
    val source = s"$instanceRoot/snapshots/$collectionId/metadata"
    val target = s"$scratchRoot/snapshots/$collectionId/metadata"
    val s = store()
    try {
      val jsons =
        s.list(source).filter(f => !f.isDirectory && f.path.endsWith(".json"))
      jsons should not be empty
      jsons.foreach { f =>
        val key = f.path.substring(f.path.indexOf(source))
        s.write(target + key.stripPrefix(source), s.readAll(key))
      }
      // The oldest copy, renamed, with an impossible segment id: its JSON
      // parses, its materialization fails.
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val first =
        jsons.map(f => f.path.substring(f.path.indexOf(source))).sorted.head
      val tree = mapper
        .readTree(s.readAll(first))
        .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      val snapshotInfo = tree
        .get("snapshot_info")
        .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      snapshotInfo.put("name", "review07-broken")
      snapshotInfo.put("create_ts", 1L)
      val items = tree
        .get("storagev2_manifest_list")
        .asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode]
      items
        .get(0)
        .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
        .put("segmentID", -1L)
      s.write(
        s"$target/0000000000000000001.json",
        mapper.writeValueAsBytes(tree)
      )
    } finally s.close()

    val base = storage(scratchRoot) ++ Map(
      StorageProperties.UseIam -> "true",
      MilvusOption.MilvusUri -> need("MILVUS_UAT_URI"),
      MilvusOption.MilvusCollectionName -> collection
    ) ++ env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
    // Before the fix the planted snapshot failed the latest read with
    // "invalid snapshot at ...: non-positive segment id(s): -1".
    spark.read.format("milvus").options(base).load().count() shouldBe 3500L
    val broken = intercept[Exception](
      spark.read
        .format("milvus")
        .options(base + (MilvusOption.ClientSnapshotName -> "review07-broken"))
        .load()
        .count()
    )
    Iterator
      .iterate[Throwable](broken)(_.getCause)
      .takeWhile(_ != null)
      .exists(t =>
        Option(t.getMessage).exists(_.contains("non-positive segment id"))
      ) shouldBe true
  }
}
