package com.zilliz.spark.connector.extensions

import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The configuration under which the takeover counts `MilvusSparkPlugin` as
  * loaded: wherever Spark loads it from.
  */
class NearestByTakeoverTest extends AnyFunSuite with Matchers {

  private val plugin = classOf[MilvusSparkPlugin].getName

  private def loaded(settings: (String, String)*): Boolean =
    NearestByTakeover.pluginLoaded(new SparkConf(false).setAll(settings))

  test("the plugin counts in spark.plugins and in spark.plugins.defaultList") {
    loaded() shouldBe false
    loaded("spark.plugins" -> "other.Plugin") shouldBe false
    loaded("spark.plugins" -> s"other.Plugin, $plugin") shouldBe true
    loaded("spark.plugins.defaultList" -> plugin) shouldBe true
    loaded(
      "spark.plugins.defaultList" -> "other.Plugin",
      "spark.plugins" -> plugin
    ) shouldBe true
  }
}
