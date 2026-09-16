package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{
  LongType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.Row

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
      ProcedureSupport.parseCollection(args.string("collection"), name)
    val options = ProcedureSupport.collectionOptions(
      args.options,
      database,
      collection,
      name
    )
    val outcome = Register.run(options, args.string("staging"))
    val status =
      if (outcome.alreadyRegistered) "already_registered" else "registered"
    outcome.items.map { item =>
      Row(outcome.jobId, item.segmentId, item.manifestVersion, status)
    }
  }

}
