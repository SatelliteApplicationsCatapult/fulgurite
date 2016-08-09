package org.catapult.sa.fulgurite.integration

import java.io.{File, FileInputStream}
import java.nio.file.Files

import org.apache.commons.io.{FileUtils, IOUtils}
import org.catapult.sa.fulgurite.spark.GeoSparkUtils
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

/**
  * simple integration tests around using the text output format
  */
class TestTextWriting {

  @Test
  def basicTextOutput() : Unit = {
    val sc = getSparkContext("test-text", "local[2]")

    val tempFile = new File(FileUtils.getTempDirectoryPath + "/test-text-output" + Random.nextInt())

    val rdd = sc.parallelize(List("hello ", "world\n", "this ", "is", " a ", "new line\n"))

    GeoSparkUtils.saveRawTextFile(rdd, tempFile.toString)
    GeoSparkUtils.joinFiles(tempFile.toString + "/out.txt", tempFile.listFiles().filter(f => f.getName.startsWith("part-")).sortBy(_.getName).toList:_*)

    val stream = new FileInputStream(tempFile.toString + "/out.txt")
    val result = IOUtils.readLines(stream)
    assertEquals("hello world", result.get(0))
    assertEquals("this is a new line", result.get(1))
    assertEquals(2, result.size())
    stream.close()

    FileUtils.forceDelete(tempFile)
  }

}
