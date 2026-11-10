package com.zilliz.milvus.storage.io

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest

/** Copies checked-in read samples without invoking a production writer. */
object ReadFixtures {
  def copyTo(name: String, directory: Path): Unit =
    copyTo(name, directory, getClass.getClassLoader)

  private[io] def copyTo(
      name: String,
      directory: Path,
      resources: ClassLoader
  ): Unit = {
    relative(name)
    val prefix = s"read-fixtures/$name/"
    val manifest = new String(read(resources, prefix + "SHA256SUMS"), UTF_8)
    val line = "([0-9a-f]{64})  (.+)".r
    val files = manifest.linesIterator
      .filter(_.nonEmpty)
      .map {
        case line(hash, name) =>
          val path = relative(name)
          require(
            name != "SHA256SUMS",
            "A fixture cannot checksum its own manifest"
          )
          (hash, name, path)
        case invalid =>
          throw new IllegalArgumentException(
            s"Invalid fixture checksum entry: $invalid"
          )
      }
      .toVector
    require(files.nonEmpty, s"Empty fixture checksum manifest: $name")
    require(
      files.map(_._2).distinct.size == files.size,
      s"Duplicate fixture path: $name"
    )

    val root = directory.toAbsolutePath.normalize()
    Files.createDirectories(root)
    files.foreach { case (expected, file, relativePath) =>
      val bytes = read(resources, prefix + file)
      val actual = MessageDigest
        .getInstance("SHA-256")
        .digest(bytes)
        .map(value => f"${value & 0xff}%02x")
        .mkString
      require(actual == expected, s"Fixture SHA-256 mismatch: $name/$file")
      val target = root.resolve(relativePath)
      require(
        target.startsWith(root),
        s"Fixture path escapes its destination: $file"
      )
      var parent = target
      while (parent != null && parent.startsWith(root)) {
        require(
          !Files.isSymbolicLink(parent),
          s"Fixture destination contains a symlink: $parent"
        )
        parent = parent.getParent
      }
      Files.createDirectories(target.getParent)
      Files.write(target, bytes)
    }
  }

  private def relative(value: String): Path = {
    require(value.matches("[A-Za-z0-9._/-]+"), s"Invalid fixture path: $value")
    val path = Paths.get(value)
    require(
      !path.isAbsolute && value
        .split("/", -1)
        .forall(part => part.nonEmpty && part != "." && part != ".."),
      s"Fixture path must be a safe relative path: $value"
    )
    path
  }

  private def read(resources: ClassLoader, path: String): Array[Byte] = {
    val input = Option(resources.getResourceAsStream(path))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Missing read fixture resource: $path"
        )
      )
    try input.readAllBytes()
    finally input.close()
  }
}
