package com.zilliz.spark.connector.extensions

import java.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** The executor plugin that loads the native bundle at executor start. */
class MilvusSparkPluginTest extends AnyFunSuite with Matchers {

  test("the plugin has an executor side and no driver side") {
    val plugin = new MilvusSparkPlugin
    plugin.driverPlugin() shouldBe null
    plugin.executorPlugin() shouldBe a[MilvusExecutorPlugin]
  }

  test("an executor without the native bundle starts anyway") {
    // The unit-test classpath carries no native bundle on most machines; with
    // one, the load succeeds. Either way init returns and the task path is
    // left to load and report on its own.
    val executor = (new MilvusSparkPlugin).executorPlugin()
    noException should be thrownBy executor.init(
      null,
      new util.HashMap[String, String]()
    )
  }
}
