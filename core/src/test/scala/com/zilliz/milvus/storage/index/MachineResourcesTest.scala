package com.zilliz.milvus.storage.index

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The machine facts a search adapts to, read from a sysfs, cgroup and procfs
  * laid out the way Linux lays them out
  * (docs/design/architecture/search-resources.html section 3.1).
  */
class MachineResourcesTest extends AnyFunSuite with Matchers {

  private def write(root: Path, relative: String, text: String): Unit = {
    val file = root.resolve(relative)
    Files.createDirectories(file.getParent)
    Files.writeString(file, text)
  }

  /** Two sockets of 32 CPUs, each with one 48M L3; every CPU also lists its own
    * L1 and L2 so the walk has to pick level 3 out.
    */
  private def twoSocketSysfs(): Path = {
    val root = Files.createTempDirectory("sysfs-cpu-")
    write(root, "online", "0-63\n")
    (0 until 64).foreach { cpu =>
      val socket = if (cpu < 32) "0-31" else "32-63"
      write(root, s"cpu$cpu/cache/index0/level", "1")
      write(root, s"cpu$cpu/cache/index0/size", "48K")
      write(root, s"cpu$cpu/cache/index0/shared_cpu_list", s"$cpu")
      write(root, s"cpu$cpu/cache/index2/level", "2")
      write(root, s"cpu$cpu/cache/index2/size", "1280K")
      write(root, s"cpu$cpu/cache/index2/shared_cpu_list", s"$cpu")
      write(root, s"cpu$cpu/cache/index3/level", "3")
      write(root, s"cpu$cpu/cache/index3/size", "49152K")
      write(root, s"cpu$cpu/cache/index3/shared_cpu_list", socket)
    }
    root
  }

  test("L3 is summed once per shared instance, whatever level 1 and 2 say") {
    val root = twoSocketSysfs()
    MachineResources.l3Bytes(root) shouldBe Some(2L * 48L * 1024 * 1024)
    MachineResources.onlineCpus(root) shouldBe Some(64)
  }

  test("a machine without cache information reports none, not zero") {
    val root = Files.createTempDirectory("sysfs-empty-")
    write(root, "online", "0-7")
    write(root, "cpu0/topology/core_id", "0")
    MachineResources.l3Bytes(root) shouldBe None
    MachineResources.onlineCpus(root) shouldBe Some(8)
    MachineResources.l3Bytes(root.resolve("missing")) shouldBe None
    MachineResources.onlineCpus(root.resolve("missing")) shouldBe None
  }

  test("sysfs sizes and cpu lists parse in the forms the kernel writes") {
    MachineResources.parseSize("49152K") shouldBe Some(49152L * 1024)
    MachineResources.parseSize("48M") shouldBe Some(48L << 20)
    MachineResources.parseSize("1G") shouldBe Some(1L << 30)
    MachineResources.parseSize("4096") shouldBe Some(4096L)
    MachineResources.parseSize("lots") shouldBe None
    MachineResources.parseSize("") shouldBe None
    MachineResources.countCpuList("0-31,64-95") shouldBe Some(64)
    MachineResources.countCpuList("0-127") shouldBe Some(128)
    MachineResources.countCpuList("3") shouldBe Some(1)
    MachineResources.countCpuList("0-3,8") shouldBe Some(5)
    MachineResources.countCpuList("") shouldBe None
    MachineResources.countCpuList("a-b") shouldBe None
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

  test("the CPU quota is the available share of the host, at most one") {
    MachineResources(Some(1L), Some(128), 8, None).cpuQuota shouldBe Some(
      8.0 / 128
    )
    MachineResources(Some(1L), Some(128), 128, None).cpuQuota shouldBe Some(
      1.0
    )
    MachineResources(Some(1L), Some(64), 128, None).cpuQuota shouldBe Some(1.0)
    MachineResources(Some(1L), None, 8, None).cpuQuota shouldBe None
  }

  test("probing the running machine yields at least the JVM's own CPU count") {
    val machine = MachineResources.probe()
    machine.availableCpus should be >= 1
    machine.l3Bytes.foreach(_ should be > 0L)
    machine.memoryLimitBytes.foreach(_ should be > 0L)
  }
}
