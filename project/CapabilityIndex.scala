import scala.util.matching.Regex

import sbt._
import sbt.Keys._

/** Checks that the capability index in the maintainers' design documents and
  * the capability ids declared in package docs still agree. The design
  * documents are kept outside this repository and are read from the directory
  * named by SPARK_MILVUS_DESIGN_DIR.
  *
  * The index is the only thing tying a feature to the code that implements it,
  * and nothing else notices when it drifts: renaming a package, moving code or
  * adding a capability all leave it silently stale. See "Keeping the index
  * true" in AGENTS.md.
  */
object CapabilityIndex {

  val checkCapabilityIndex =
    taskKey[Unit]("Verify capabilities.md and the package docs still agree")

  /** Path of the capability index inside the design documents. */
  private val CapabilitiesDoc =
    "projects/spark-milvus/capabilities.md"

  /** Names the directory holding the maintainers' design documents. */
  private val DesignDirVariable = "SPARK_MILVUS_DESIGN_DIR"

  /** The design documents, from the environment. */
  private def designDir: File =
    sys.env
      .get(DesignDirVariable)
      .filter(_.nonEmpty)
      .map(file)
      .getOrElse(
        sys.error(
          s"$DesignDirVariable is not set: point it at the directory holding " +
            "the maintainers' design documents"
        )
      )

  /** A row in one of the capability tables: `| R1 | ... | ... | location | ...`
    */
  private val RowPattern: Regex =
    raw"^\|\s*([RWCAVKOG]\d+)\s*\|([^|]*)\|([^|]*)\|([^|]*)\|".r

  /** A row in section 11: `| R19 | reason |` */
  private val UnplacedPattern: Regex =
    raw"^\|\s*([RWCAVKOG]\d+)\s*\|[^|]*\|\s*$$".r

  private val IdPattern: Regex = raw"\b([RWCAVKOG]\d+)\b".r

  /** The design names used in the 实现位置 column, and the package they mean.
    * `spark.X` resolves to shared sources and all supported shim roots; any
    * source-bearing matching package counts.
    */
  private val Prefixes: Seq[(String, Seq[String])] = Seq(
    "core." -> Seq("core/src/main/scala/com/zilliz/milvus/storage"),
    "native-storage." -> Seq(
      "milvus-storage/java/src/main/scala/io/milvus/storage",
      "milvus-storage/java/src/main/java/io/milvus/storage"
    ),
    "native-vector." -> Seq(
      "native-vector/src/main/java/com/zilliz/milvus/jni/vector"
    ),
    "spark." -> Seq(
      "spark/src/main/scala/com/zilliz/spark/connector",
      "spark/src/main/scala-shims/spark-3.5/com/zilliz/spark/connector",
      "spark/src/main/scala-shims/spark-3.5-4.1/com/zilliz/spark/connector",
      "spark/src/main/scala-shims/spark-4.0-4.1/com/zilliz/spark/connector",
      "spark/src/main/scala-shims/spark-4.2/com/zilliz/spark/connector"
    )
  )

  /** Splits section 11 off before parsing, so its rows are not read as
    * capability rows.
    */
  private def sections(doc: String): (String, String) = {
    val marker = "\n## 11 "
    val at = doc.indexOf(marker)
    if (at < 0) (doc, "") else (doc.substring(0, at), doc.substring(at))
  }

  // Regex pattern matching anchors the whole string, and these patterns only
  // cover the leading columns, so match explicitly.
  private def declaredIds(body: String): Map[String, String] =
    body.linesIterator
      .flatMap(line => RowPattern.findFirstMatchIn(line))
      .map(m => m.group(1) -> m.group(4).trim)
      .toMap

  private def unplacedIds(section11: String): Set[String] =
    section11.linesIterator
      .flatMap(line => UnplacedPattern.findFirstMatchIn(line))
      .map(_.group(1))
      .toSet

  /** Whether `dir` holds a source file other than its own package doc. */
  private def hasSource(dir: File): Boolean =
    (dir * ("*.scala" | "*.java")).get.exists { f =>
      f.getName != "package.scala" && f.getName != "package-info.java"
    }

  /** Every package doc, and the ids it claims. */
  private def packageDocIds(root: File): Map[File, Set[String]] = {
    val docs =
      Prefixes.flatMap(_._2).map(root / _).filter(_.isDirectory).flatMap {
        dir =>
          (dir ** ("package.scala" | "package-info.java")).get
      }
    docs.map(f => f -> claimedIn(IO.read(f))).toMap
  }

  /** The ids a package doc claims: those inside its `Capabilities: ... (see
    * design document capabilities.md)` sentence, which may wrap across lines.
    * Ids anywhere else are prose — "Storage V2", "DataSource V2", "the RPC
    * behind C1" — and must not count as a claim.
    */
  private val ClaimSentence: Regex =
    raw"(?s)Capabilities:(.*?)\(see[\s*]+design[\s*]+document[\s*]+capabilities\.md\)".r

  private def claimedIn(doc: String): Set[String] =
    ClaimSentence
      .findFirstMatchIn(doc)
      .map(m => IdPattern.findAllMatchIn(m.group(1)).map(_.group(1)).toSet)
      .getOrElse(Set.empty)

  /** Resolves one 实现位置 entry to the directories it could mean. An entry that
    * names no package at all (prose such as "spark 层汇总") resolves to nothing
    * and is skipped.
    */
  private def resolve(root: File, name: String): Seq[File] =
    Prefixes
      .collectFirst {
        case (prefix, roots) if name.startsWith(prefix) =>
          val sub = name.stripPrefix(prefix).replace('.', '/')
          roots.map(root / _ / sub)
      }
      .getOrElse(Seq.empty)

  /** Pulls the package names out of a 实现位置 cell, which is prose with package
    * names embedded: "spark.read 的 SupportsReportPartitioning →
    * core.read.plan".
    */
  private def locationsIn(cell: String): Seq[String] = {
    val token =
      raw"(?<![A-Za-z0-9_.])((?:core|compat|client|apps|spark|native-storage|native-vector)\.[a-zA-Z][a-zA-Z0-9.]*)".r
    token.findAllMatchIn(cell).map(_.group(1).stripSuffix(".")).toSeq.distinct
  }

  val settings: Seq[Setting[_]] = Seq(
    checkCapabilityIndex := {
      val log = streams.value.log
      val root = (ThisBuild / baseDirectory).value
      val doc = designDir / CapabilitiesDoc
      if (!doc.isFile)
        sys.error(
          s"$doc not found: $DesignDirVariable must name the directory holding " +
            "the maintainers' design documents"
        )

      val (body, section11) = sections(IO.read(doc))
      val declared = declaredIds(body)
      val unplaced = unplacedIds(section11)
      val byDoc = packageDocIds(root)
      val claimed = byDoc.values.flatten.toSet

      val problems = Seq.newBuilder[String]

      declared.keys.toSeq.sorted.foreach { id =>
        if (!claimed.contains(id) && !unplaced.contains(id)) {
          problems += s"$id is in $CapabilitiesDoc but no package.scala carries it, " +
            "and it is not declared in section 11"
        }
      }

      unplaced.toSeq.sorted.foreach { id =>
        if (claimed.contains(id)) {
          problems += s"$id is declared unplaced in section 11 but a package.scala " +
            "now carries it; remove the section 11 row"
        }
        if (!declared.contains(id)) {
          problems += s"$id is in section 11 but has no capability row"
        }
      }

      byDoc.toSeq.sortBy(_._1.getPath).foreach { case (file, ids) =>
        ids.toSeq.sorted.foreach { id =>
          if (!declared.contains(id)) {
            problems += s"${file.relativeTo(root).getOrElse(file)} claims $id, " +
              s"which has no row in $CapabilitiesDoc"
          }
        }
        // A package doc is the index's claim that the code for these ids is
        // here. A directory holding nothing but the doc cannot make that claim:
        // until code lands, the ids belong in section 11 instead.
        if (ids.nonEmpty && !hasSource(file.getParentFile)) {
          problems += s"${file.relativeTo(root).getOrElse(file)} claims " +
            s"${ids.toSeq.sorted.mkString(", ")} but its package has no source " +
            "file; move the ids to section 11 until code lands there"
        }
      }

      declared.toSeq.sortBy(_._1).foreach { case (id, cell) =>
        locationsIn(cell).foreach { name =>
          val candidates = resolve(root, name)
          if (candidates.nonEmpty && !candidates.exists(_.isDirectory)) {
            problems += s"$id names package `$name`, which does not exist on disk"
          }
        }
      }

      val found = problems.result()
      if (found.nonEmpty) {
        found.foreach(log.error(_))
        sys.error(
          s"capability index is out of date: ${found.size} problems " +
            "(see \"Keeping the index true\" in AGENTS.md)"
        )
      }
      log.info(
        s"capability index ok: ${declared.size} capabilities, " +
          s"${claimed.size} carried by ${byDoc.count(_._2.nonEmpty)} package docs, " +
          s"${unplaced.size} declared unplaced"
      )
    }
  )
}
