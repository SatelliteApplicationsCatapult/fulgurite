package org.catapult.sa.fulgurite.integration

import java.io.{File, FileInputStream}

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import org.apache.commons.io.{FileUtils, IOUtils}
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.GeoSparkUtils
import org.junit.Assert._
import org.junit.Test

import scala.util.Random

/**
  * NOTE: On windows this test requires the hadoop win binary
  */
class TestWriting {

  @Test
  def testBasicWrite() : Unit = {

    val outputName = FileUtils.getTempDirectoryPath + "/tmp" + Random.nextInt()
    new File(outputName).deleteOnExit()

    val sc = getSparkContext("TestWriting", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_NONE,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    val input = sc.parallelize((0 until 11).flatMap(y => (0 until 11).flatMap(x => List(Index(x, y, 0) -> 255, Index(x, y, 1) -> 128, Index(x, y, 2) -> 0))))

    GeoSparkUtils.saveGeoTiff(input, metaData, outputName, 10)
    GeoSparkUtils.joinOutputFiles( outputName + "/header.tiff", outputName + "/", outputName + "/data.tif")

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

    val sc = getSparkContext("TestWriting", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_NONE,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    metaData.planarConfiguration = BaselineTIFFTagSet.PLANAR_CONFIGURATION_PLANAR

    val input = for {
      band <- 0 until 3
      y <- 0 until 11
      x <- 0 until 11
    } yield Index(x, y, band) -> band

    val inputRDD = sc.parallelize(input)

    GeoSparkUtils.saveGeoTiff(inputRDD, metaData, outputName, 10)
    GeoSparkUtils.joinOutputFiles( outputName + "/header.tiff", outputName + "/", outputName + "/data.tif")

    val result = new Array[Byte](4000)
    val expected = new Array[Byte](4000)

    IOUtils.read(new FileInputStream(outputName + "/data.tif"), result)
    IOUtils.read(new FileInputStream("src/test/resources/data_planar.tif"), expected)

    assertArrayEquals(expected, result)

  }

  @Test
  def addingTransparentSample() : Unit = {
    val outputName = FileUtils.getTempDirectoryPath + "/tmp" + Random.nextInt()
    new File(outputName).deleteOnExit()

    val sc = getSparkContext("TestWriting", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_NONE,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    metaData.planarConfiguration = BaselineTIFFTagSet.PLANAR_CONFIGURATION_PLANAR
    metaData.samplesPerPixel = metaData.samplesPerPixel + 1 // One extra sample
    metaData.bitsPerSample = (8 :: metaData.bitsPerSample.toList).toArray // that is 8 bits
    metaData.extraSamples = Array(BaselineTIFFTagSet.EXTRA_SAMPLES_UNASSOCIATED_ALPHA)
    metaData.sampleFormat = (BaselineTIFFTagSet.SAMPLE_FORMAT_UNSIGNED_INTEGER :: metaData.sampleFormat.toList).toArray

    val input = for {
      band <- 0 until 4
      y <- 0 until 11
      x <- 0 until 11
    } yield Index(x, y, band) -> (if (band == 3) 255 else band)

    val inputRDD = sc.parallelize(input)

    GeoSparkUtils.saveGeoTiff(inputRDD, metaData, outputName, 10)
    GeoSparkUtils.joinOutputFiles( outputName + "/header.tiff", outputName + "/", outputName + "/data.tif")

    val result = new Array[Byte](4000)
    val expected = new Array[Byte](4000)

    IOUtils.read(new FileInputStream(outputName + "/data.tif"), result)
    IOUtils.read(new FileInputStream("src/test/resources/data_planar_transparent.tif"), expected)

    assertArrayEquals(expected, result)
  }


  @Test(expected = classOf[IllegalArgumentException])
  def failCompressedWriting() : Unit = {
    val sc = getSparkContext("TestWriting", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_JPEG,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    val inputRDD = sc.emptyRDD[(Index, Int)]

    GeoSparkUtils.saveGeoTiff(inputRDD, metaData, "wibble", 10)
  }
}
