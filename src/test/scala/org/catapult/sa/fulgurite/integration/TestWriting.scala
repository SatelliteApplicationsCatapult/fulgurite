package org.catapult.sa.fulgurite.integration

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{GeoSparkUtils, SparkUtils}
import org.junit.Test
import org.junit.Assert._

import scala.util.Random

/**
  * NOTE: On windows this test requires the hadoop win binary
  */
class TestWriting {

  @Test
  def testBasicWrite() : Unit = {

    val outputName = FileUtils.getTempDirectoryPath + "/tmp" + Random.nextInt()
    new File(outputName).deleteOnExit()

    val conf = SparkUtils.createConfig("TestWriting", "local[2]")
    val sc = SparkContext.getOrCreate(conf)

    val (metaData, baseMeta) = GeoTiffMeta("src/test/resources/data_chunked.tif")

    val input = sc.parallelize((0 until 11).flatMap(y => (0 until 11).flatMap(x => List(Index(x, y, 0) -> 255, Index(x, y, 1) -> 128, Index(x, y, 2) -> 0))))

    GeoSparkUtils.saveGeoTiff(input, metaData, baseMeta, outputName, 10)

    SparkUtils.joinOutputFiles( outputName + "/header.tiff", outputName + "/", outputName + "/data.tif")

    val result = new Array[Byte](4000)
    val expected = new Array[Byte](4000)
    IOUtils.read(new FileInputStream(outputName + "/data.tif"), result)
    IOUtils.read(new FileInputStream("src/test/resources/data_chunked.tif"), expected)

    assertArrayEquals(expected, result)

  }


  @Test
  def testWritePlanar() : Unit = {
    val outputName = FileUtils.getTempDirectoryPath + "/tmp" + Random.nextInt()
    new File(outputName).deleteOnExit()

    val conf = SparkUtils.createConfig("TestWriting", "local[2]")
    val sc = SparkContext.getOrCreate(conf)

    val (meta, baseMeta) = GeoTiffMeta("src/test/resources/data_chunked.tif") // Blarg get hold of base meta.

    val metaData = GeoTiffMeta(11, 11, 3, meta.bitsPerSample, meta.startOffset, meta.endOffset, meta.tiePoints, meta.pixelScales, meta.colourMode, 2)

    val input = for {
      band <- 0 until 3
      y <- 0 until 11
      x <- 0 until 11
    } yield Index(x, y, band) -> band

    val inputRDD = sc.parallelize(input)

    GeoSparkUtils.saveGeoTiff(inputRDD, metaData, baseMeta, outputName, 10)
    SparkUtils.joinOutputFiles( outputName + "/header.tiff", outputName + "/", outputName + "/data.tif")

    val result = new Array[Byte](4000)
    val expected = new Array[Byte](4000)
    IOUtils.read(new FileInputStream(outputName + "/data.tif"), result)
    IOUtils.read(new FileInputStream("src/test/resources/data_planar.tif"), expected)

    assertArrayEquals(expected, result)

  }

}
