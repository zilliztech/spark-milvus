package com.zilliz.milvus.storage.io

import java.net.URLClassLoader
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.MessageDigest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ReadFixturesTest extends AnyFunSuite with Matchers {
  private val bytes = "fixed input".getBytes(UTF_8)
  private val hash = MessageDigest
    .getInstance("SHA-256")
    .digest(bytes)
    .map(value => f"${value & 0xff}%02x")
    .mkString

  private def withResources(
      manifest: String
  )(body: (Path, URLClassLoader) => Unit): Unit = {
    val root = Files.createTempDirectory("read-fixture-resources")
    val fixture = root.resolve("read-fixtures/example")
    Files.createDirectories(fixture.resolve("nested"))
    Files.write(fixture.resolve("SHA256SUMS"), manifest.getBytes(UTF_8))
    Files.write(fixture.resolve("nested/input.bin"), bytes)
    val loader = new URLClassLoader(Array(root.toUri.toURL), null)
    try body(Files.createTempDirectory("read-fixture-copy"), loader)
    finally loader.close()
  }

  test("copies the exact checked bytes from classpath resources") {
    withResources(s"$hash  nested/input.bin\n") { (destination, loader) =>
      ReadFixtures.copyTo("example", destination, loader)
      Files.readAllBytes(destination.resolve("nested/input.bin")) shouldBe bytes
    }
  }

  test(
    "a changed payload and a missing payload fail rather than reuse local files"
  ) {
    Seq(s"${"0" * 64}  nested/input.bin\n", s"$hash  absent.bin\n")
      .foreach { manifest =>
        withResources(manifest) { (destination, loader) =>
          an[IllegalArgumentException] should be thrownBy
            ReadFixtures.copyTo("example", destination, loader)
        }
      }
  }

  test("unsafe, repeated and malformed manifest paths are rejected") {
    Seq(
      "../input.bin",
      "/input.bin",
      "nested/../input.bin",
      "nested//input.bin",
      "a\\b"
    )
      .map(path => s"$hash  $path\n")
      .++(
        Seq(
          s"$hash  nested/input.bin\n$hash  nested/input.bin\n",
          "not-a-checksum\n",
          ""
        )
      )
      .foreach { manifest =>
        withResources(manifest) { (destination, loader) =>
          an[IllegalArgumentException] should be thrownBy
            ReadFixtures.copyTo("example", destination, loader)
        }
      }
  }

  test(
    "a destination symlink cannot redirect a fixture outside its directory"
  ) {
    withResources(s"$hash  nested/input.bin\n") { (destination, loader) =>
      val outside = Files.createTempDirectory("read-fixture-outside")
      Files.createSymbolicLink(destination.resolve("nested"), outside)
      an[IllegalArgumentException] should be thrownBy
        ReadFixtures.copyTo("example", destination, loader)
      Files.exists(outside.resolve("input.bin")) shouldBe false
    }
  }

  test("fixture names cannot escape the read fixture resource directory") {
    val destination = Files.createTempDirectory("read-fixture-invalid-name")
    Seq("../example", "/example", "", "example//nested").foreach { name =>
      an[IllegalArgumentException] should be thrownBy
        ReadFixtures.copyTo(name, destination)
    }
  }
}
