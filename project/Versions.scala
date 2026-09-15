/** Application dependency versions and the Spark compatibility matrix. */
object Versions {

  val scala212 = "2.12.20"
  val scala213 = "2.13.16"

  /** `id` is the directory suffix; `projectId` is the sbt id without dots. */
  final case class SparkLine(
      id: String,
      projectId: String,
      spark: String,
      arrow: String,
      antlr: String,
      javaRelease: String,
      scalas: Seq[String]
  )

  /** Compile against the compatibility floor, with the Arrow version that Spark
    * ships. The runtime test matrix is a separate choice.
    */
  val lines: Seq[SparkLine] = Seq(
    SparkLine(
      "3.5",
      "spark35",
      "3.5.5",
      "12.0.1",
      "4.9.3",
      "11",
      Seq(scala212, scala213)
    ),
    SparkLine(
      "4.0",
      "spark40",
      "4.0.0",
      "18.1.0",
      "4.13.1",
      "17",
      Seq(scala213)
    ),
    SparkLine(
      "4.1",
      "spark41",
      "4.1.1",
      "18.3.0",
      "4.13.1",
      "17",
      Seq(scala213)
    ),
    SparkLine(
      "4.2",
      "spark42",
      "4.2.0",
      "19.0.0",
      "4.13.1",
      "17",
      Seq(scala213)
    )
  )

  def line(id: String): SparkLine =
    lines.find(_.id == id).getOrElse(sys.error(s"unknown Spark line: $id"))

  /** Shared modules must remain loadable on the 3.5 line's Java baseline. */
  val sharedJavaRelease = "11"
  val sharedScalas: Seq[String] = Seq(scala212, scala213)

  val scalapb = "0.11.3"
  val grpcJava = "1.37.0"
  val parquet = "1.13.1"
  val parquetAvro = "1.15.2"
  val hadoop = "3.4.1"
  val avro = "1.12.0"
  val awsSdkV2 = "2.30.38"
  val awsSdkV1Core = "1.12.780"
  val munit = "0.7.29"
  val scalaTest = "3.2.15"

  /** Keep jackson-module-scala and jackson-databind on the same version. */
  val jackson = "2.17.3"

  /** xxh3, the hash Milvus's primary-key bloom filter is keyed by. */
  val zeroAllocationHashing = "0.16"

  /** The logging API provided by Spark 3.5 and compatible later lines. */
  val slf4j = "2.0.7"

  /** Supplies scala.jdk.CollectionConverters on Scala 2.12. */
  val scalaCollectionCompat = "2.12.0"

  /** Retained for the root's existing assembly; Spark modules use their line.
    */
  val legacyRootArrow = "17.0.0"
}
