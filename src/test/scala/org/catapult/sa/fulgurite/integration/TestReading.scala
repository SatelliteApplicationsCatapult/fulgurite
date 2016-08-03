package org.catapult.sa.fulgurite.integration

import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.GeoSparkUtils
import org.junit.Assert._
import org.junit.Test

/**
  * A big test to make sure that reading works as expected.
  */
class TestReading {

  @Test
  def basicReadingTest(): Unit = {
    val sc = getSparkContext("basicReadingTest", "local[2]")

    val metaData = GeoTiffMeta("src/test/resources/data_chunked.tif")
    val result = GeoSparkUtils.GeoTiffRDD("src/test/resources/data_chunked.tif", metaData, sc, 10 )
      .map(_._2)
      .collect()

    val expected = (0 until 11).flatMap(y => (0 until 11).flatMap(x => List(255, 128, 0))).toArray

    assertArrayEquals(expected, result)
  }
}
