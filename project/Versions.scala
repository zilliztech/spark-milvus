/** The 2.0 version matrix.
  *
  * Each Spark line pins its own Spark, Arrow, antlr and Java version. The
  * modules shared across lines (core, compat, client) compile against the
  * lowest line. See section 4 of docs/design/modules.md.
  */
object Versions {

  val scala212 = "2.12.20"
  val scala213 = "2.13.16"

  /** One Spark line that is still maintained.
    *
    * @param id
    *   The directory name and artifact suffix, for example "3.5".
    * @param projectId
    *   The sbt project id. It cannot contain a dot.
    */
  final case class SparkLine(
      id: String,
      projectId: String,
      spark: String,
      arrow: String,
      antlr: String,
      javaRelease: String,
      scalas: Seq[String]
  )

  /** Spark 3.4 no longer gets releases, so it is out. The interface differences
    * across the three 4.x lines are close to zero.
    *
    * The Spark patch is the lowest maintained one on each line, not the newest:
    * the version you compile against is the compatibility floor, and pinning a
    * higher patch raises that floor with it. Which patches actually get tested
    * is a separate matrix.
    *
    * Arrow matches what the line's Spark ships (arrow.version in Spark's
    * spark-parent pom). Diverging loads two copies of Arrow on the executor.
    */
  val lines: Seq[SparkLine] = Seq(
    SparkLine("3.5", "spark35", "3.5.5", "15.0.2", "4.9.3", "11", Seq(scala212, scala213)),
    SparkLine("4.0", "spark40", "4.0.0", "18.1.0", "4.13.1", "17", Seq(scala213)),
    SparkLine("4.1", "spark41", "4.1.1", "18.3.0", "4.13.1", "17", Seq(scala213)),
    SparkLine("4.2", "spark42", "4.2.0", "19.0.0", "4.13.1", "17", Seq(scala213))
  )

  /** The lowest line. `lines` is ordered from oldest to newest, and spark-base
    * compiles against this one.
    */
  val lowest: SparkLine = lines.head

  def line(id: String): SparkLine =
    lines.find(_.id == id).getOrElse(sys.error(s"unknown Spark line: $id"))

  /** Shared modules compile to the 3.5 line's floor. Anything higher and a 3.5
    * user gets UnsupportedClassVersionError on class load.
    */
  val sharedJavaRelease = "11"
  val sharedScalas: Seq[String] = Seq(scala212, scala213)

  /** jackson-module-scala and jackson-databind have to be the same version;
    * mixing them throws JsonMappingException outright. parquet-hadoop pulls in
    * a newer databind transitively.
    */
  val jackson = "2.17.3"

  /** Compiled against the 3.5 line's floor. The 4.x lines ship something newer
    * with the same API.
    */
  val slf4j = "2.0.7"

  /** Backfills scala.jdk.CollectionConverters for 2.12. See constraint 5 in
    * section 4 of modules.md.
    */
  val scalaCollectionCompat = "2.12.0"
}
