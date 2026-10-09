package com.zilliz.spark.connector.options

import org.apache.spark.resource.ResourceProfile
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The task cores a stage declares for itself (docs/design/architecture/
  * vector-search.html section 1.1): every core for a stage of Knowhere calls,
  * enough to bound how many run at once for a stage bounded by the heap, and
  * nothing where Spark would not run it or the default already does.
  */
class TaskResourcesTest extends AnyFunSuite with Matchers {

  private def cores(profile: Option[ResourceProfile]): Option[Double] =
    profile.map(_.taskResources(ResourceProfile.CPUS).amount)

  test("a Knowhere stage takes every core, so one task runs per executor") {
    val resources = TaskResources(2, 8, 1, declarable = true)
    cores(resources.wholeExecutor) shouldBe Some(8.0)
    resources.tasksPerExecutor shouldBe 8
  }

  test("nothing is declared where the default already takes every core") {
    TaskResources(2, 8, 8, declarable = true).wholeExecutor shouldBe None
    TaskResources(2, 8, 8, declarable = true).tasksPerExecutor shouldBe 1
  }

  test("nothing is declared on a local master or with dynamic allocation") {
    TaskResources(1, 8, 1, declarable = false).wholeExecutor shouldBe None
    TaskResources(1, 8, 1, declarable = false).atMost(2) shouldBe None
  }

  test("a stage bounded by the heap takes the cores that bound it") {
    val resources = TaskResources(2, 8, 1, declarable = true)
    // Four at once: two cores each. Three at once: three cores, so two run.
    cores(resources.atMost(4)) shouldBe Some(2.0)
    cores(resources.atMost(3)) shouldBe Some(3.0)
    cores(resources.atMost(1)) shouldBe Some(8.0)
    // Eight or more at once is what the default already runs.
    resources.atMost(8) shouldBe None
    resources.atMost(100) shouldBe None
  }
}
