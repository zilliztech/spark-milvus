package com.zilliz.spark.connector.read

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.{
  AdaptiveSparkPlanExec,
  QueryStageExec
}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.SparkPlan
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.snapshot.SnapshotCatalog
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SnapshotSources,
  StorageOptions,
  V2SegmentResolvers
}

/** The whole read chain on a real snapshot in the UAT bucket: the snapshot JSON
  * is read through `SnapshotCatalog` on the driver, planned into partitions,
  * and every partition is read by the native reader on a local Spark executor.
  * No Milvus service is involved (capability R3).
  *
  * Cancels unless the environment names the snapshot:
  * {{{
  *   MILVUS_JNI_S3_BUCKET=bucket
  *   MILVUS_JNI_S3_REGION=us-west-2            # optional
  *   MILVUS_JNI_S3_ENDPOINT=s3.us-west-2.amazonaws.com   # optional
  *   MILVUS_JNI_S3_ROOT_PATH=<instance root path>      # client mode: where snapshots/ lives
  *   MILVUS_UAT_SNAPSHOT_PATH=files/snapshots/<coll>/metadata/<id>.json
  *   MILVUS_UAT_EXPECTED_ROWS=12345           # optional
  *   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  * }}}
  */
class SnapshotReadUatTest extends AnyFunSuite with Matchers {

  private def env(n: String): Option[String] =
    sys.env.get(n).map(_.trim).filter(_.nonEmpty)

  private def storageOptions(): Map[String, String] = {
    val bucket = env("MILVUS_JNI_S3_BUCKET").getOrElse(
      cancel(
        "set MILVUS_JNI_S3_BUCKET, MILVUS_UAT_SNAPSHOT_PATH and AWS_* to reach the UAT bucket"
      )
    )
    val region = env("MILVUS_JNI_S3_REGION").getOrElse("us-west-2")
    val endpoint =
      env("MILVUS_JNI_S3_ENDPOINT").getOrElse(s"s3.$region.amazonaws.com")
    Map(
      StorageProperties.BucketName -> bucket,
      StorageProperties.Address -> endpoint,
      StorageProperties.Region -> region,
      StorageProperties.CloudProvider -> "aws",
      StorageProperties.UseSSL -> "true",
      StorageProperties.UseIam -> "true"
    ) ++ env("MILVUS_JNI_S3_ROOT_PATH").map(StorageProperties.RootPath -> _)
  }

  private def snapshotPath(): String =
    env("MILVUS_UAT_SNAPSHOT_PATH").getOrElse(
      cancel(
        "set MILVUS_UAT_SNAPSHOT_PATH to a snapshot JSON in the UAT bucket"
      )
    )

  /** Not part of the read. The other cases need a collection with flushed
    * segments and a snapshot of it; this makes both through `client.api`
    * (capabilities C2 and A1) when `MILVUS_UAT_URI` is set, and prints the
    * snapshot location for `MILVUS_UAT_SNAPSHOT_PATH`.
    */
  test(
    "prepare: a collection with flushed rows and a snapshot of it (C2, A1)"
  ) {
    import io.milvus.grpc.schema._
    val uri = env("MILVUS_UAT_URI").getOrElse(
      cancel(
        "set MILVUS_UAT_URI (and MILVUS_UAT_TOKEN) to prepare a collection"
      )
    )
    val collection = env("MILVUS_UAT_COLLECTION").getOrElse(
      "spark_uat_" + System.currentTimeMillis()
    )
    val rows = env("MILVUS_UAT_ROWS").map(_.toInt).getOrElse(3000)
    val dim = 4
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    try {
      if (client.getCollectionInfo("", collection).isFailure) {
        val schema = client.createCollectionSchema(
          name = collection,
          fields = Seq(
            client.createCollectionField(
              "id",
              isPrimary = true,
              dataType = DataType.Int64
            ),
            client.createCollectionField(
              "name",
              dataType = DataType.VarChar,
              typeParams = Map("max_length" -> "64")
            ),
            client.createCollectionField(
              "v",
              dataType = DataType.FloatVector,
              typeParams = Map("dim" -> dim.toString)
            )
          )
        )
        client
          .createCollection(collectionName = collection, schema = schema)
          .get
        this.info(s"created collection $collection")
        val batch = 1000
        (0 until rows by batch).foreach { start =>
          val ids = (start until math.min(start + batch, rows)).map(_.toLong)
          val fields = Seq(
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
              `type` = DataType.VarChar,
              fieldName = "name",
              field = FieldData.Field.Scalars(
                ScalarField(data =
                  ScalarField.Data.StringData(
                    StringArray(data = ids.map(i => s"row-$i"))
                  )
                )
              )
            ),
            FieldData(
              `type` = DataType.FloatVector,
              fieldName = "v",
              field = FieldData.Field.Vectors(
                VectorField(
                  dim = dim,
                  data = VectorField.Data.FloatVector(
                    FloatArray(data =
                      ids.flatMap(i =>
                        (0 until dim).map(d => (i * 10 + d).toFloat)
                      )
                    )
                  )
                )
              )
            )
          )
          client
            .insert(
              collectionName = collection,
              fieldsData = fields,
              numRows = ids.size
            )
            .get
        }
        client.flush(collectionNames = Seq(collection)).get
        this.info(s"inserted $rows rows and flushed")
      }
      // Flush is asynchronous on the service and GetPersistentSegmentInfo is a
      // denied API on Zilliz Cloud, so the wait is a fixed pause; whether the
      // snapshot then holds every row is what the read cases check.
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val name = "spark_uat_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT read",
          86400L
        )
        .get
      this.info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      this.info(
        s"export MILVUS_UAT_COLLECTION=$collection MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location}"
      )
    } finally client.close()
  }

  /** Deletes rows of the prepared collection and takes a new snapshot, so the
    * read cases can check that deleted rows stay out (capability R8). Needs
    * `MILVUS_UAT_URI`, `MILVUS_UAT_COLLECTION` and `MILVUS_UAT_DELETE_IDS`
    * (comma-separated primary keys).
    */
  test("prepare: delete rows and take a new snapshot (R8)") {
    val uri = env("MILVUS_UAT_URI").getOrElse(
      cancel("set MILVUS_UAT_URI (and MILVUS_UAT_TOKEN) to delete rows")
    )
    val collection = env("MILVUS_UAT_COLLECTION").getOrElse(
      cancel("set MILVUS_UAT_COLLECTION to the prepared collection")
    )
    val ids = env("MILVUS_UAT_DELETE_IDS")
      .map(_.split(",").map(_.trim.toInt).toSeq)
      .getOrElse(cancel("set MILVUS_UAT_DELETE_IDS to the ids to delete"))
    val client = com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )
    try {
      client
        .delete[Int](
          collectionName = collection,
          pkName = Some("id"),
          pks = ids
        )
        .get
      client.flush(collectionNames = Seq(collection)).get
      this.info(s"deleted ${ids.size} rows of $collection and flushed")
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val name = "spark_uat_del_" + System.currentTimeMillis()
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          name,
          "spark-milvus UAT delete read",
          86400L
        )
        .get
      this.info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      this.info(
        s"export MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location} MILVUS_UAT_DELETED_IDS=${ids.mkString(",")}"
      )
    } finally client.close()
  }

  private def clientOf(uri: String) =
    com.zilliz.milvus.client.api.MilvusClient(
      MilvusOption(
        Map(MilvusOption.MilvusUri -> uri) ++
          env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
      ).connectionParams
    )

  private def rowsOf(ids: Seq[Long], dim: Int) = {
    import io.milvus.grpc.schema._
    Seq(
      FieldData(
        `type` = DataType.Int64,
        fieldName = "id",
        field = FieldData.Field.Scalars(
          ScalarField(data = ScalarField.Data.LongData(LongArray(data = ids)))
        )
      ),
      FieldData(
        `type` = DataType.VarChar,
        fieldName = "name",
        field = FieldData.Field.Scalars(
          ScalarField(data =
            ScalarField.Data.StringData(
              StringArray(data = ids.map(i => s"row-$i"))
            )
          )
        )
      ),
      FieldData(
        `type` = DataType.FloatVector,
        fieldName = "v",
        field = FieldData.Field.Vectors(
          VectorField(
            dim = dim,
            data = VectorField.Data.FloatVector(
              FloatArray(data =
                ids.flatMap(i => (0 until dim).map(d => (i * 10 + d).toFloat))
              )
            )
          )
        )
      )
    )
  }

  /** A collection with three partitions, one of which holds two segments: 1000
    * rows in `_default`, 1000 + 500 in `p1` (two flushes), 1000 in `p2`. Prints
    * the snapshot for the partition and segment cases below.
    */
  test("prepare: three partitions, one with two segments (R16)") {
    import io.milvus.grpc.schema._
    val uri = env("MILVUS_UAT_URI").getOrElse(
      cancel(
        "set MILVUS_UAT_URI (and MILVUS_UAT_TOKEN) to prepare a collection"
      )
    )
    val collection =
      env("MILVUS_UAT_PARTS_COLLECTION").getOrElse(
        cancel("set MILVUS_UAT_PARTS_COLLECTION")
      )
    val dim = 4
    val client = clientOf(uri)
    try {
      if (client.getCollectionInfo("", collection).isSuccess) {
        cancel(
          s"$collection exists; drop it or point MILVUS_UAT_PARTS_COLLECTION elsewhere"
        )
      }
      val schema = client.createCollectionSchema(
        name = collection,
        fields = Seq(
          client.createCollectionField(
            "id",
            isPrimary = true,
            dataType = DataType.Int64
          ),
          client.createCollectionField(
            "name",
            dataType = DataType.VarChar,
            typeParams = Map("max_length" -> "64")
          ),
          client.createCollectionField(
            "v",
            dataType = DataType.FloatVector,
            typeParams = Map("dim" -> dim.toString)
          )
        )
      )
      client.createCollection(collectionName = collection, schema = schema).get
      client
        .createPartition(collectionName = collection, partitionName = "p1")
        .get
      client
        .createPartition(collectionName = collection, partitionName = "p2")
        .get
      def insert(partition: Option[String], ids: Seq[Long]): Unit =
        client
          .insert(
            collectionName = collection,
            partitionName = partition,
            fieldsData = rowsOf(ids, dim),
            numRows = ids.size
          )
          .get
      insert(None, 0L until 1000L)
      insert(Some("p1"), 1000L until 2000L)
      insert(Some("p2"), 2000L until 3000L)
      client.flush(collectionNames = Seq(collection)).get
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      insert(Some("p1"), 3000L until 3500L)
      client.flush(collectionNames = Seq(collection)).get
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          "spark_uat_parts_" + System.currentTimeMillis(),
          "spark-milvus UAT partitions",
          86400L
        )
        .get
      this.info(s"created snapshot ${snapshot.name} at ${snapshot.s3Location}")
      this.info(
        s"export MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location} MILVUS_UAT_EXPECTED_ROWS=3500"
      )
    } finally client.close()
  }

  /** Client mode: the service names the collection, the snapshot directory
    * supplies the latest snapshot, and the partition and segment selectors
    * narrow it. Needs the partitioned collection above.
    */
  test(
    "client mode reads the latest snapshot and narrows by partition and by segment"
  ) {
    val uri = env("MILVUS_UAT_URI").getOrElse(cancel("set MILVUS_UAT_URI"))
    val collection =
      env("MILVUS_UAT_PARTS_COLLECTION").getOrElse(
        cancel("set MILVUS_UAT_PARTS_COLLECTION")
      )
    val base = storageOptions() ++ Map(
      MilvusOption.MilvusUri -> uri,
      MilvusOption.MilvusCollectionName -> collection
    ) ++ env("MILVUS_UAT_TOKEN").map(MilvusOption.MilvusToken -> _)
    val snapshot = SnapshotSources
      .forRead(MilvusOption(base), withSegments = true)
      .snapshot()
      .fold(e => throw e, identity)
    info(
      s"latest snapshot ${snapshot.name}: ${snapshot.partitionIds.size} partitions, " +
        s"${snapshot.dataSegments.size} data segments (${snapshot.v3Segments.size} V3)"
    )
    snapshot.partitionIds.size shouldBe 3
    snapshot.dataSegments.size should be >= 4
    withSpark { spark =>
      def count(extra: (String, String)*): Long =
        spark.read.format("milvus").options(base ++ extra).load().count()
      count() shouldBe 3500L
      val perPartition = snapshot.partitionIds.map { partitionId =>
        partitionId -> count(
          MilvusOption.MilvusPartitions -> partitionId.toString
        )
      }
      perPartition.foreach(_._2 should be > 0L)
      perPartition.map(_._2).sum shouldBe 3500L
      val firstTwo = perPartition.take(2)
      count(
        MilvusOption.MilvusPartitions -> firstTwo.map(_._1).mkString(",")
      ) shouldBe firstTwo.map(_._2).sum
      // Every data segment read alone, and the parts add up to the whole.
      val perSegment = snapshot.dataSegments.map { seg =>
        val n = count(MilvusOption.MilvusSegments -> seg.id.toString)
        info(s"segment ${seg.id} (partition ${seg.partitionId}): $n rows")
        n should be > 0L
        n
      }
      perSegment.sum shouldBe 3500L
      an[Exception] should be thrownBy count(
        MilvusOption.MilvusPartitions -> Long.MaxValue.toString
      )
      an[Exception] should be thrownBy count(
        MilvusOption.MilvusSegments -> Long.MaxValue.toString
      )
    }
  }

  /** Column pruning and the metadata columns, on `MILVUS_UAT_SNAPSHOT_PATH`. */
  test("projection and metadata columns") {
    storageOptions(); snapshotPath()
    val expected = env("MILVUS_UAT_EXPECTED_ROWS").map(_.toLong)
    withSpark { spark =>
      import org.apache.spark.sql.functions._
      val df = read(spark)
      val names = df.select("id", "name").collect()
      expected.foreach(e => names.length.toLong shouldBe e)
      names.foreach(r => r.getString(1) shouldBe s"row-${r.getLong(0)}")
      val vectors = df.select("id", "v").limit(5).collect()
      vectors.foreach { r =>
        val v = r.getSeq[Float](1)
        v.length shouldBe 4
        v.head shouldBe (r.getLong(0) * 10).toFloat
      }
      val withMeta = read(
        spark,
        MilvusOption.MilvusExtraColumns ->
          "_segment_id,_row_offset,_timestamp"
      )
      val bySegment = withMeta
        .groupBy(col("_segment_id"))
        .agg(
          count("*").as("n"),
          min("_row_offset").as("lo"),
          max("_row_offset").as("hi"),
          min("_timestamp").as("first_ts")
        )
        .collect()
      bySegment.foreach { r =>
        info(s"segment ${r.getLong(0)}: ${r.getLong(1)} rows, offsets ${r
            .getLong(2)}..${r.getLong(3)}, first timestamp ${r.getLong(4)}")
        // _row_offset is the row's position in the segment, so deleted rows
        // leave holes: the range covers at least n rows and, without
        // deletes, exactly n.
        val (n, lo, hi) = (r.getLong(1), r.getLong(2), r.getLong(3))
        (hi - lo + 1) should be >= n
        if (env("MILVUS_UAT_DELETED_IDS").isEmpty) {
          lo shouldBe 0L
          hi shouldBe n - 1
        }
      }
      expected.foreach(e => bySegment.map(_.getLong(1)).sum shouldBe e)
      // Columnar read with a projection delivers the same rows.
      val columnar = read(spark, MilvusOption.ReadColumnar -> "true")
        .select("id", "name")
        .collect()
      columnar.length shouldBe names.length
    }
  }

  /** A collection with representative scalar fields, an Int64 array, JSON, a
    * nullable column, a float vector and a binary vector; 100 rows with values
    * derived from the id, so the read case can check every column. Needs
    * `MILVUS_UAT_TYPES_COLLECTION`.
    */
  test("prepare: representative scalar, array, nullable and vector fields") {
    import io.milvus.grpc.schema._
    import com.google.protobuf.ByteString
    val uri = env("MILVUS_UAT_URI").getOrElse(cancel("set MILVUS_UAT_URI"))
    val collection =
      env("MILVUS_UAT_TYPES_COLLECTION").getOrElse(
        cancel("set MILVUS_UAT_TYPES_COLLECTION")
      )
    val client = clientOf(uri)
    try {
      if (client.getCollectionInfo("", collection).isSuccess) {
        cancel(
          s"$collection exists; drop it or point MILVUS_UAT_TYPES_COLLECTION elsewhere"
        )
      }
      val f = client.createCollectionField _
      val schema = client.createCollectionSchema(
        name = collection,
        fields = Seq(
          client.createCollectionField(
            "id",
            isPrimary = true,
            dataType = DataType.Int64
          ),
          client.createCollectionField("b", dataType = DataType.Bool),
          client.createCollectionField("i8", dataType = DataType.Int8),
          client.createCollectionField("i16", dataType = DataType.Int16),
          client.createCollectionField("i32", dataType = DataType.Int32),
          client.createCollectionField("f", dataType = DataType.Float),
          client.createCollectionField("d", dataType = DataType.Double),
          client.createCollectionField(
            "s",
            dataType = DataType.VarChar,
            typeParams = Map("max_length" -> "64")
          ),
          client.createCollectionField("j", dataType = DataType.JSON),
          client.createCollectionField(
            "arr",
            dataType = DataType.Array,
            elementType = DataType.Int64,
            typeParams = Map("max_capacity" -> "4")
          ),
          client.createCollectionField(
            "opt",
            dataType = DataType.Int32,
            nullable = true
          ),
          client.createCollectionField(
            "v",
            dataType = DataType.FloatVector,
            typeParams = Map("dim" -> "4")
          ),
          client.createCollectionField(
            "bv",
            dataType = DataType.BinaryVector,
            typeParams = Map("dim" -> "16")
          )
        )
      )
      client.createCollection(collectionName = collection, schema = schema).get
      val ids = (0L until 100L).toSeq
      def scalars(name: String, dt: DataType, data: ScalarField.Data) =
        FieldData(
          `type` = dt,
          fieldName = name,
          field = FieldData.Field.Scalars(ScalarField(data = data))
        )
      val valid = ids.map(i => i % 3 != 0)
      val fields = Seq(
        scalars(
          "id",
          DataType.Int64,
          ScalarField.Data.LongData(LongArray(data = ids))
        ),
        scalars(
          "b",
          DataType.Bool,
          ScalarField.Data.BoolData(BoolArray(data = ids.map(_ % 2 == 0)))
        ),
        scalars(
          "i8",
          DataType.Int8,
          ScalarField.Data.IntData(
            IntArray(data = ids.map(i => (i % 100).toInt))
          )
        ),
        scalars(
          "i16",
          DataType.Int16,
          ScalarField.Data.IntData(
            IntArray(data = ids.map(i => (i * 100).toInt))
          )
        ),
        scalars(
          "i32",
          DataType.Int32,
          ScalarField.Data.IntData(
            IntArray(data = ids.map(i => (i * 1000).toInt))
          )
        ),
        scalars(
          "f",
          DataType.Float,
          ScalarField.Data.FloatData(FloatArray(data = ids.map(_ * 0.5f)))
        ),
        scalars(
          "d",
          DataType.Double,
          ScalarField.Data.DoubleData(DoubleArray(data = ids.map(_ * 0.25)))
        ),
        scalars(
          "s",
          DataType.VarChar,
          ScalarField.Data.StringData(
            StringArray(data = ids.map(i => s"row-$i"))
          )
        ),
        scalars(
          "j",
          DataType.JSON,
          ScalarField.Data.JsonData(
            JSONArray(data =
              ids.map(i => ByteString.copyFromUtf8(s"""{"k":$i,"tag":"t$i"}"""))
            )
          )
        ),
        scalars(
          "arr",
          DataType.Array,
          ScalarField.Data.ArrayData(
            ArrayArray(
              data = ids.map(i =>
                ScalarField(data =
                  ScalarField.Data.LongData(
                    LongArray(data = Seq(i, i + 1, i + 2))
                  )
                )
              ),
              elementType = DataType.Int64
            )
          )
        ),
        scalars(
          "opt",
          DataType.Int32,
          ScalarField.Data.IntData(
            IntArray(data = ids.filter(_ % 3 != 0).map(_.toInt))
          )
        )
          .copy(validData = valid),
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
        ),
        FieldData(
          `type` = DataType.BinaryVector,
          fieldName = "bv",
          field = FieldData.Field.Vectors(
            VectorField(
              dim = 16,
              data = VectorField.Data.BinaryVector(
                ByteString.copyFrom(
                  ids
                    .flatMap(i =>
                      Seq((i & 0xff).toByte, ((i >> 8) & 0xff).toByte)
                    )
                    .toArray
                )
              )
            )
          )
        )
      )
      client
        .insert(
          collectionName = collection,
          fieldsData = fields,
          numRows = ids.size
        )
        .get
      client.flush(collectionNames = Seq(collection)).get
      Thread.sleep(
        env("MILVUS_UAT_FLUSH_WAIT_MS").map(_.toLong).getOrElse(20000L)
      )
      val snapshot = client
        .createSnapshotForRead(
          "",
          collection,
          "spark_uat_types_" + System.currentTimeMillis(),
          "spark-milvus UAT types",
          86400L
        )
        .get
      this.info(
        s"export MILVUS_UAT_SNAPSHOT_PATH=${snapshot.s3Location} MILVUS_UAT_EXPECTED_ROWS=100"
      )
    } finally client.close()
  }

  /** Every column of the representative types collection reads back with the
    * value it was written with on both reader shapes. Needs
    * `MILVUS_UAT_SNAPSHOT_PATH` of that collection.
    */
  test("representative types read back on row and columnar paths") {
    storageOptions(); snapshotPath()
    if (!env("MILVUS_UAT_TYPES_SNAPSHOT").contains("true")) {
      cancel(
        "set MILVUS_UAT_TYPES_SNAPSHOT=true when MILVUS_UAT_SNAPSHOT_PATH is the types collection"
      )
    }
    withSpark { spark =>
      def assertValues(path: String, collected: Array[Row]): Unit = {
        val rows = collected
          .map(row => row.getLong(row.fieldIndex("id")) -> row)
          .toMap
        withClue(s"$path reader: ") {
          rows.size shouldBe 100
        }
        Seq(0L, 1L, 2L, 3L, 50L, 99L).foreach { i =>
          val row = rows(i)
          withClue(s"$path reader, row $i: ") {
            row.getBoolean(row.fieldIndex("b")) shouldBe (i % 2 == 0)
            row.getByte(row.fieldIndex("i8")).toLong shouldBe i % 100
            row.getShort(row.fieldIndex("i16")).toLong shouldBe i * 100
            row.getInt(row.fieldIndex("i32")).toLong shouldBe i * 1000
            row.getFloat(row.fieldIndex("f")) shouldBe i * 0.5f
            row.getDouble(row.fieldIndex("d")) shouldBe i * 0.25
            row.getString(row.fieldIndex("s")) shouldBe s"row-$i"
            row.getString(row.fieldIndex("j")) should include(s""""k":$i""")
            row.getSeq[Long](row.fieldIndex("arr")) shouldBe Seq(
              i,
              i + 1,
              i + 2
            )
            if (i % 3 == 0) row.isNullAt(row.fieldIndex("opt")) shouldBe true
            else row.getInt(row.fieldIndex("opt")).toLong shouldBe i
            row.getSeq[Float](row.fieldIndex("v")) shouldBe (0 until 4).map(
              dimension => (i * 10 + dimension).toFloat
            )
            row.getAs[Array[Byte]](row.fieldIndex("bv")).toSeq shouldBe Seq(
              (i & 0xff).toByte,
              ((i >> 8) & 0xff).toByte
            )
          }
        }
      }

      val columnar = read(spark)
      info(s"schema: ${columnar.schema.treeString}")
      assertValues("columnar", columnar.collect())
      assertValues(
        "row",
        read(spark, MilvusOption.ReadColumnar -> "false").collect()
      )
    }
  }

  test("SnapshotCatalog reads the snapshot JSON through the native store") {
    val options = storageOptions()
    val path = snapshotPath()
    val bucket = options(StorageProperties.BucketName)
    val store = StorageOptions.storeFor(
      StorageOptions.buildHadoopConfForOptions(options, ""),
      bucket,
      options
    )
    val snapshot =
      new SnapshotCatalog(store, bucket, V2SegmentResolvers.footer(true))
        .read(path)
    info(
      s"snapshot ${snapshot.name}: collection ${snapshot.collectionId}, " +
        s"${snapshot.partitionIds.size} partitions, ${snapshot.segments.size} segments " +
        s"(${snapshot.v3Segments.size} V3, ${snapshot.v2Segments.size} V2, " +
        s"${snapshot.deleteOnlySegments.size} delete-only), " +
        s"${snapshot.schema.fields.size} fields"
    )
    snapshot.dataSegments should not be empty
    snapshot.primaryKeyField should not be empty
    snapshot.segments.foreach(seg =>
      info(
        s"segment ${seg.id} v${seg.storageVersion} rows=${seg.rows} deletes=${seg.deletes}"
      )
    )
  }

  private def withSpark(f: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("snapshot-read-uat")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    try f(spark)
    finally spark.stop()
  }

  private def read(spark: SparkSession, extra: (String, String)*) = {
    var reader = spark.read
      .format("milvus")
      .option(MilvusOption.SnapshotPath, snapshotPath())
    storageOptions().foreach { case (k, v) => reader = reader.option(k, v) }
    extra.foreach { case (k, v) => reader = reader.option(k, v) }
    reader.load()
  }

  test("format(\"milvus\") with milvus.snapshot.path reads every row") {
    storageOptions(); snapshotPath()
    withSpark { spark =>
      val df = read(spark)
      info(s"schema: ${df.schema.treeString}")
      val rows = df.count()
      info(s"row reader delivered $rows rows")
      rows should be > 0L
      env("MILVUS_UAT_EXPECTED_ROWS").foreach(e => rows shouldBe e.toLong)
      val sample = df.limit(3).collect()
      sample.foreach(r => info(r.toString.take(200)))
      // Rows deleted before the snapshot was taken must not come back.
      env("MILVUS_UAT_DELETED_IDS").foreach { ids =>
        val deleted = ids.split(",").map(_.trim.toLong).toSeq
        val present =
          df.filter(org.apache.spark.sql.functions.col("id").isin(deleted: _*))
            .count()
        info(s"${deleted.size} deleted ids, $present of them present")
        present shouldBe 0L
      }
    }
  }

  /** The G5 metrics come back through the scan node of the executed plan: the
    * row path converts every surviving row, the columnar path none.
    */
  test("the read's metrics reach the scan node (G5)") {
    storageOptions(); snapshotPath()
    withSpark { spark =>
      def scanMetrics(columnar: Boolean): Map[String, Long] = {
        val df =
          read(spark, MilvusOption.ReadColumnar -> columnar.toString)
        val counted = df.groupBy().count()
        val rows = counted.collect().head.getLong(0)
        // Adaptive execution wraps the plan and hides each finished stage's
        // subtree behind a QueryStageExec, so the walk has to open both.
        def scansIn(plan: SparkPlan): Seq[BatchScanExec] = plan match {
          case adaptive: AdaptiveSparkPlanExec => scansIn(adaptive.executedPlan)
          case stage: QueryStageExec           => scansIn(stage.plan)
          case scan: BatchScanExec             => Seq(scan)
          case other => other.children.flatMap(scansIn)
        }
        val scans = scansIn(counted.queryExecution.executedPlan)
        scans should have size 1
        val metrics = scans.head.metrics.collect {
          case (name, metric) if name.startsWith("milvus.") =>
            name -> metric.value
        }
        info(s"columnar=$columnar rows=$rows metrics=${metrics.toSeq.sorted}")
        metrics
      }
      val row = scanMetrics(columnar = false)
      row("milvus.jni.calls") should be > 0L
      row("milvus.jni.nanos") should be > 0L
      row("milvus.arrow.batches") should be > 0L
      row("milvus.arrow.bytes") should be > 0L
      row("milvus.arrow.allocated.max") should be > 0L
      env("MILVUS_UAT_EXPECTED_ROWS").foreach { e =>
        row("milvus.rows.materialized") shouldBe e.toLong
      }
      val columnar = scanMetrics(columnar = true)
      columnar("milvus.rows.materialized") shouldBe 0L
      columnar("milvus.arrow.batches") shouldBe row("milvus.arrow.batches")
    }
  }

  test("the columnar reader delivers the same row count") {
    storageOptions(); snapshotPath()
    withSpark { spark =>
      val rowCount = read(spark, MilvusOption.ReadColumnar -> "false").count()
      val columnar = read(spark).count()
      info(s"row path $rowCount rows, columnar path (default) $columnar rows")
      columnar shouldBe rowCount
    }
  }
}
