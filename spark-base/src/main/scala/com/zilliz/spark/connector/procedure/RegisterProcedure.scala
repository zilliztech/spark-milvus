package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

import com.zilliz.spark.connector.options.MilvusOption

/** `CALL milvus.system.register('db.coll', staging => '{root}/staging/{job}')`:
  * hands a committed write job's segments to Milvus (capability A4). The body
  * is `Register.run`; this is its parameter list and result table.
  */
object RegisterProcedure extends Procedure {
  override val name: String = "register"

  override val parameters: Seq[Parameter] = Seq(
    Parameter("collection", StringType),
    Parameter("staging", StringType)
  )

  override val outputSchema: StructType = StructType(
    Seq(
      StructField("job_id", StringType, nullable = false),
      StructField("segment_id", LongType, nullable = false),
      StructField("manifest_version", LongType, nullable = false),
      StructField("status", StringType, nullable = false)
    )
  )

  override def run(args: ProcedureArgs): Seq[Row] = {
    val (database, collection) =
      RegisterProcedure.splitCollection(args.string("collection"))
    val options = args.options ++
      database.map(MilvusOption.MilvusDatabaseName -> _) +
      (MilvusOption.MilvusCollectionName -> collection)
    val outcome = Register.run(options, args.string("staging"))
    val status =
      if (outcome.alreadyRegistered) "already_registered" else "registered"
    outcome.items.map { item =>
      Row(outcome.jobId, item.segmentId, item.manifestVersion, status)
    }
  }

  /** `'db.coll'` names both; `'coll'` leaves the database to the connection's
    * default. Neither name may contain a dot in Milvus, so the first dot
    * splits.
    */
  private[procedure] def splitCollection(
      value: String
  ): (Option[String], String) = {
    val trimmed = value.trim
    if (trimmed.isEmpty) {
      throw new IllegalArgumentException(
        "procedure register: collection must not be empty"
      )
    }
    trimmed.indexOf('.') match {
      case -1 => (None, trimmed)
      case i =>
        val db = trimmed.substring(0, i)
        val coll = trimmed.substring(i + 1)
        if (db.isEmpty || coll.isEmpty || coll.contains('.')) {
          throw new IllegalArgumentException(
            s"procedure register: collection must be 'db.coll' or 'coll', got '$value'"
          )
        }
        (Some(db), coll)
    }
  }
}
