package com.zilliz.milvus.storage.vector

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The memory limit read from cgroup and procfs laid out as Linux writes them
  * (docs/design/architecture/search-resources.html section 3.1).
  */
class MachineResourcesTest extends AnyFunSuite with Matchers {

  private def write(root: Path, relative: String, text: String): Unit = {
    val file = root.resolve(relative)
    Files.createDirectories(file.getParent)
    Files.writeString(file, text)
  }

  test("the memory limit is the cgroup's when it has one, else the machine's") {
    val root = Files.createTempDirectory("cgroup-")
    val meminfo = root.resolve("meminfo")
    Files.writeString(
      meminfo,
      "MemTotal:       527966408 kB\nMemFree:        1 kB\n",
      UTF_8
    )
    val total = 527966408L * 1024L
    // v2 without a limit
    write(root, "v2max/memory.max", "max\n")
    MachineResources.memoryLimitBytes(root.resolve("v2max"), meminfo) shouldBe
      Some(total)
    // v2 with a limit below the total
    write(root, "v2/memory.max", "8589934592\n")
    MachineResources.memoryLimitBytes(root.resolve("v2"), meminfo) shouldBe
      Some(8589934592L)
    // v1 with the kernel's "no limit" sentinel, above the total
    write(root, "v1/memory/memory.limit_in_bytes", "9223372036854771712\n")
    MachineResources.memoryLimitBytes(root.resolve("v1"), meminfo) shouldBe
      Some(total)
    // nothing readable at all
    MachineResources.memoryLimitBytes(
      root.resolve("none"),
      root.resolve("no-meminfo")
    ) shouldBe None
  }

  test("the running process's memory limit is positive when available") {
    MachineResources.memoryLimitBytes().foreach(_ should be > 0L)
  }
}
