package com.zilliz.milvus.storage.vector

import java.nio.file.{Files, Path, Paths}
import scala.util.Try

/** The memory limit the process runs under, read from Linux cgroup and procfs.
  * A value that cannot be read is None, so the caller decides the fallback
  * (docs/design/projects/spark-milvus/architecture/search-resources.html
  * section 3.1).
  */
object MachineResources {

  /** Reads the running process's memory limit. */
  def memoryLimitBytes(): Option[Long] =
    memoryLimitBytes(
      Paths.get("/sys/fs/cgroup"),
      Paths.get("/proc/meminfo")
    )

  /** The cgroup v2 `memory.max`, else the cgroup v1
    * `memory/memory.limit_in_bytes`, else `MemTotal` from `meminfo`. A cgroup
    * that says `max` or a limit above the machine's total means no limit, and
    * the machine's total is returned.
    */
  def memoryLimitBytes(cgroupDir: Path, meminfo: Path): Option[Long] = {
    val total = read(meminfo).flatMap { text =>
      text.linesIterator
        .find(_.startsWith("MemTotal:"))
        .flatMap(line => Try(line.split("\\s+")(1).toLong * 1024L).toOption)
    }
    val v2 = read(cgroupDir.resolve("memory.max"))
      .filterNot(_ == "max")
      .flatMap(text => Try(text.toLong).toOption)
    val v1 = read(cgroupDir.resolve("memory").resolve("memory.limit_in_bytes"))
      .flatMap(text => Try(text.toLong).toOption)
    val limit = v2.orElse(v1).filter(_ > 0L)
    (limit, total) match {
      case (Some(l), Some(t)) => Some(math.min(l, t))
      case (Some(l), None)    => Some(l).filter(_ < Long.MaxValue / 2)
      case (None, t)          => t
    }
  }

  private def read(file: Path): Option[String] =
    if (Files.isRegularFile(file))
      Try(Files.readString(file).trim).toOption.filter(_.nonEmpty)
    else None
}
