package org.catapult.sa.fulgurite.integration

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.GeoSparkUtils
import org.junit.Assert._
import org.junit.Test

/**
  * A big test to make sure that reading works as expected.
  */
class TestReading {

  @Test
  def chunkedReadingTest(): Unit = {
    val sc = getSparkContext("basicReadingTest", "local[2]")

    val metaData = GeoTiffMeta("src/test/resources/data_chunked.tif")
    val result = GeoSparkUtils.GeoTiffRDD("src/test/resources/data_chunked.tif", metaData, sc, 10 )
      .map(_._2)
      .collect()

    val expected = (0 until 11).flatMap(y => (0 until 11).flatMap(x => List(255, 128, 0))).toArray

    assertArrayEquals(expected, result)
  }

  @Test
  def planarReadingTest(): Unit = {
    val sc = getSparkContext("basicReadingTest", "local[2]")

    val metaData = GeoTiffMeta("src/test/resources/data_planar.tif")
    val result = GeoSparkUtils.GeoTiffRDD("src/test/resources/data_planar.tif", metaData, sc, 10 )
      .map(_._2)
      .collect()

    val expected = for {
      band <- 0 until 3
      y <- 0 until 11
      x <- 0 until 11
    } yield if (band == 3) 255 else band

    assertArrayEquals(expected.toArray, result)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def tryReadCompressed() : Unit = {
    val sc = getSparkContext("basicReadingTest", "local[2]")
    val metaData = GeoTiffMeta("src/test/resources/data_planar.tif")
    metaData.compression = BaselineTIFFTagSet.COMPRESSION_JPEG
    GeoSparkUtils.GeoTiffRDD("src/test/resources/data_planar.tif", metaData, sc, 10 )

  }
}
