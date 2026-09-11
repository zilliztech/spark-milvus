/** 2.0 的版本矩阵。
  *
  * 每条 Spark 线钉自己的 Spark、Arrow、antlr 和 Java 版本；跨线共用的模块
  * （core、compat、client）按最低的那条线编译。见 docs/design/modules.md 第 4 节。
  */
object Versions {

  val scala212 = "2.12.20"
  val scala213 = "2.13.16"

  /** 一条维护中的 Spark 线。
    *
    * @param id
    *   目录名与 artifact 后缀，如 "3.5"
    * @param projectId
    *   sbt 的 project id，不能带点
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

  /** Spark 3.4 已停止发版，不做。4.x 三条线的接口差异接近零。
    *
    * spark 取每条线最低的维护 patch，不取最新：编译版本就是兼容下限，钉高位会把
    * 下限一起抬上去。跑哪些 patch 是测试矩阵的事，另表。
    *
    * arrow 与本线 Spark 自带的对齐（Spark 的 spark-parent pom 里的 arrow.version），
    * 否则 executor 上会加载两份 Arrow。
    */
  val lines: Seq[SparkLine] = Seq(
    SparkLine("3.5", "spark35", "3.5.5", "15.0.2", "4.9.3", "11", Seq(scala212, scala213)),
    SparkLine("4.0", "spark40", "4.0.0", "18.1.0", "4.13.1", "17", Seq(scala213)),
    SparkLine("4.1", "spark41", "4.1.1", "18.3.0", "4.13.1", "17", Seq(scala213)),
    SparkLine("4.2", "spark42", "4.2.0", "19.0.0", "4.13.1", "17", Seq(scala213))
  )

  /** 最低的那条线。lines 按版本从低到高排，spark-base 钉它编译。 */
  val lowest: SparkLine = lines.head

  def line(id: String): SparkLine =
    lines.find(_.id == id).getOrElse(sys.error(s"unknown Spark line: $id"))

  /** 共用模块按 3.5 线的下限编译，否则 3.5 的用户加载即报 UnsupportedClassVersionError。 */
  val sharedJavaRelease = "11"
  val sharedScalas: Seq[String] = Seq(scala212, scala213)

  /** jackson-module-scala 与 jackson-databind 必须同版本，跨版本直接抛
    * JsonMappingException。parquet-hadoop 会传递一个更新的 databind 进来。
    */
  val jackson = "2.17.3"

  /** 按 3.5 线的下限编译；4.x 自带的更新，接口一样。 */
  val slf4j = "2.0.7"

  /** 为 2.12 补齐 scala.jdk.CollectionConverters，见 modules.md 第 4 节第 5 条。 */
  val scalaCollectionCompat = "2.12.0"
}
