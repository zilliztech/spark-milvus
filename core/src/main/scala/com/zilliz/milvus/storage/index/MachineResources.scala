package com.zilliz.milvus.storage.index

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** What the machine a task runs on offers it: its L3 caches, its CPUs, and the
  * memory limit the process runs under. Read from Linux sysfs, cgroup and
  * procfs; a value that cannot be read is None and is never guessed, so the
  * caller decides the fallback (docs/design/architecture/search-resources.html
  * section 3.1).
  *
  * @param l3Bytes
  *   the sum over the distinct L3 instances, each counted once however many
  *   CPUs share it
  * @param hostCpus
  *   the CPUs the host has online, which sysfs reports from inside a container
  *   too
  * @param availableCpus
  *   the CPUs this JVM may use, which Java 21 reads from the cgroup CPU quota
  * @param memoryLimitBytes
  *   the cgroup memory limit, or the machine's total when there is none
  */
final case class MachineResources(
    l3Bytes: Option[Long],
    hostCpus: Option[Int],
    availableCpus: Int,
    memoryLimitBytes: Option[Long]
) {

  /** The share of the host this process may use: available CPUs over host CPUs,
    * at most 1. None when the host count is unknown.
    */
  def cpuQuota: Option[Double] =
    hostCpus
      .filter(_ > 0)
      .map(host => math.min(1.0, availableCpus.toDouble / host))
}

object MachineResources {

  /** Reads the running machine. */
  def probe(): MachineResources =
    probe(
      Paths.get("/sys/devices/system/cpu"),
      Paths.get("/sys/fs/cgroup"),
      Paths.get("/proc/meminfo"),
      Runtime.getRuntime.availableProcessors()
    )

  def probe(
      cpuDir: Path,
      cgroupDir: Path,
      meminfo: Path,
      availableCpus: Int
  ): MachineResources =
    MachineResources(
      l3Bytes(cpuDir),
      onlineCpus(cpuDir),
      math.max(1, availableCpus),
      memoryLimitBytes(cgroupDir, meminfo)
    )

  /** Sums the L3 caches under `cpuDir` (`/sys/devices/system/cpu`): every
    * `cpuN/cache/indexM` with `level` 3, one instance per distinct
    * `shared_cpu_list`.
    */
  def l3Bytes(cpuDir: Path): Option[Long] = Try {
    val instances = scala.collection.mutable.LinkedHashMap.empty[String, Long]
    children(cpuDir).filter(_.getFileName.toString.matches("cpu\\d+")).foreach {
      cpu =>
        children(cpu.resolve("cache")).foreach { index =>
          if (read(index.resolve("level")).contains("3")) {
            for {
              shared <- read(index.resolve("shared_cpu_list"))
              size <- read(index.resolve("size")).flatMap(parseSize)
            } instances.getOrElseUpdate(shared, size)
          }
        }
    }
    instances.values.sum
  }.toOption.filter(_ > 0L)

  /** The CPUs listed in `cpuDir/online`, such as `0-127` or `0-3,8-11`. */
  def onlineCpus(cpuDir: Path): Option[Int] =
    read(cpuDir.resolve("online")).flatMap(countCpuList)

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

  /** `49152K`, `48M`, `1G` or a plain byte count, as sysfs writes cache sizes.
    */
  private[index] def parseSize(text: String): Option[Long] = {
    val trimmed = text.trim.toUpperCase(java.util.Locale.ROOT)
    if (trimmed.isEmpty) None
    else {
      val (digits, unit) = trimmed.span(_.isDigit)
      Try(digits.toLong).toOption.flatMap { n =>
        unit match {
          case ""  => Some(n)
          case "K" => Some(n << 10)
          case "M" => Some(n << 20)
          case "G" => Some(n << 30)
          case _   => None
        }
      }
    }
  }

  /** The number of CPUs a sysfs list such as `0-31,64-95` names. */
  private[index] def countCpuList(text: String): Option[Int] = Try {
    text.trim
      .split(",")
      .filter(_.nonEmpty)
      .map { part =>
        part.split("-") match {
          case Array(single)   => 1
          case Array(from, to) => to.trim.toInt - from.trim.toInt + 1
          case _               => throw new IllegalArgumentException(part)
        }
      }
      .sum
  }.toOption.filter(_ > 0)

  private def children(dir: Path): Seq[Path] =
    if (Files.isDirectory(dir))
      Try(Files.list(dir).iterator.asScala.toVector).getOrElse(Vector.empty)
    else Vector.empty

  private def read(file: Path): Option[String] =
    if (Files.isRegularFile(file))
      Try(Files.readString(file).trim).toOption.filter(_.nonEmpty)
    else None
}
