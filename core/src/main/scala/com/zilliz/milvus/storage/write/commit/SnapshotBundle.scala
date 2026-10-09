package com.zilliz.milvus.storage.write.commit

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper

import com.zilliz.milvus.storage.io.ObjectStore
import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentEntry,
  SnapshotSegmentReader
}
import com.zilliz.milvus.storage.path.StoragePath
import com.zilliz.milvus.storage.snapshot.json.ManifestContentJson

/** What Milvus's external restore requires of a snapshot's paths.
  *
  * A restore derives a root from the snapshot document's own key — the prefix
  * above `snapshots/{collection}/metadata/{id}.json` — and refuses any file the
  * snapshot names outside it (`ValidateExternalSnapshotPaths`, so that a
  * tampered document cannot point a restore at someone else's files). A
  * snapshot meant for restore therefore has to be written under a prefix the
  * data already sits below. This object states the rule once, so that
  * `write_snapshot` refuses at planning what a restore would refuse at run
  * time, and `restore_snapshot` checks before it asks
  * (docs/design/architecture/vector-search.html section 2.7).
  */
object SnapshotBundle {

  private val mapper = new ObjectMapper()

  private val MetadataKey =
    """^(?:(.*)/)?snapshots/\d+/metadata/\d+\.json$""".r

  /** The root a restore derives from a snapshot document's key, or None when
    * the key is not laid out as
    * `{root}/snapshots/{collection}/metadata/{id}.json`. A document directly
    * under the bucket has the empty root.
    */
  def rootOf(metadataKey: String): Option[String] =
    Option(metadataKey).map(_.trim.stripPrefix("/")).flatMap {
      case MetadataKey(root) => Some(Option(root).getOrElse(""))
      case _                 => None
    }

  /** Every storage path a snapshot names: the V3 segments' base paths from
    * `storagev2_manifest_list`, and the binlog, delta, statistics and index
    * files of each segment manifest.
    */
  def pathsOf(
      document: ObjectNode,
      entries: Seq[SnapshotSegmentEntry]
  ): Seq[String] = {
    val basePaths = Seq.newBuilder[String]
    Option(document.get("storagev2_manifest_list"))
      .filter(_.isArray)
      .foreach { items =>
        items.elements().forEachRemaining { item =>
          Option(item.get("manifest"))
            .map(_.asText(""))
            .filter(_.nonEmpty)
            .foreach { manifest =>
              basePaths += ManifestContentJson
                .parse(manifest)
                .map(_.basePath)
                .getOrElse(manifest)
            }
        }
      }
    val fromManifests = entries.flatMap { entry =>
      (entry.binlogFiles ++ entry.deltaLogFiles ++ entry.statsLogFiles)
        .flatMap(_.binlogs.map(_.logPath)) ++
        entry.indexFiles.toSeq.flatten.flatMap(_.filePaths)
    }
    (basePaths.result() ++ fromManifests).filter(_.nonEmpty)
  }

  /** The paths among `paths` that are not under `root`, as they were given.
    *
    * A path is read the way [[StoragePath.parse]] reads user input, so
    * `s3://b/k` and `k` name the same file, and a path in another bucket is
    * outside. The empty root covers the whole bucket.
    */
  def outsideRoot(
      root: String,
      paths: Iterable[String],
      bucket: String
  ): Seq[String] = {
    val prefix = normalized(root)
    paths.toSeq.distinct.filterNot { raw =>
      val located = StoragePath.parse(raw, bucket)
      val sameBucket =
        !located.hasBucket || bucket.isEmpty || located.bucket == bucket
      val key = located.key.stripPrefix("/")
      sameBucket && (prefix.isEmpty || key == prefix || key.startsWith(
        prefix + "/"
      ))
    }
  }

  /** Reads the snapshot document at `metadataKey` and its segment manifests and
    * returns the paths a restore would refuse: those outside the root the key
    * derives, the manifests themselves included. Fails when the key is not laid
    * out as a snapshot document or a manifest cannot be read.
    */
  def outsideRootOf(
      store: ObjectStore,
      metadataKey: String,
      bucket: String
  ): Seq[String] = {
    val root = rootOf(metadataKey).getOrElse(
      throw new IllegalArgumentException(
        s"'$metadataKey' is not laid out as '<root>/snapshots/<collection>/metadata/<id>.json', " +
          "so no restore root derives from it"
      )
    )
    val document = mapper.readTree(store.readAll(metadataKey)) match {
      case node: ObjectNode => node
      case other =>
        throw new IllegalArgumentException(
          s"The snapshot document at '$metadataKey' is not a JSON object: ${other.getNodeType}"
        )
    }
    val schemaVersion =
      Option(document.get("format_version")).map(_.asInt(1)).getOrElse(1)
    val manifestKeys = texts(document, "manifest_list")
    val entries = manifestKeys.map { key =>
      SnapshotSegmentReader.parse(store.readAll(key), schemaVersion) match {
        case Right(entry) => entry
        case Left(failure) =>
          throw new IllegalArgumentException(
            s"The segment manifest at '$key' cannot be read: ${failure.getMessage}",
            failure
          )
      }
    }
    outsideRoot(root, manifestKeys ++ pathsOf(document, entries), bucket)
  }

  /** One line naming what is outside, for the refusal a caller raises. */
  def describeOutside(root: String, outside: Seq[String]): String = {
    val shown = outside.take(5).mkString(", ")
    val more = if (outside.size > 5) s" and ${outside.size - 5} more" else ""
    s"${outside.size} path(s) are outside the root '${normalized(root)}': $shown$more"
  }

  private def normalized(root: String): String =
    Option(root).map(_.trim.stripPrefix("/").stripSuffix("/")).getOrElse("")

  private def texts(document: ObjectNode, field: String): Seq[String] = {
    val values = Seq.newBuilder[String]
    Option(document.get(field)).filter(_.isArray).foreach { array =>
      array.elements().forEachRemaining(value => values += value.asText())
    }
    values.result().filter(_.nonEmpty)
  }
}
