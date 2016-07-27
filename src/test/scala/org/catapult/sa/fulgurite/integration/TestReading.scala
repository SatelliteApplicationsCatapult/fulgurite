package org.catapult.sa.fulgurite.integration

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{GeoSparkUtils, SparkUtils}
import org.junit.Test
import org.junit.Assert._

/**
  * A bunch of quite big tests to
  */
class TestReading {

  @Test
  def basicReadingTest(): Unit = {

    val conf = SparkUtils.createConfig("basicReadingTest", "local[2]")
    val sc = new SparkContext(conf)

    val (metaData, _) = GeoTiffMeta("src/test/resources/data_chunked.tif")
    val result = GeoSparkUtils.GeoTiffRDD("src/test/resources/data_chunked.tif", metaData, sc, 10 )
      .map(_._2)
      .collect()

    val expected = (0 until 11).flatMap(y => (0 until 11).flatMap(x => List(255, 128, 0))).toArray

    assertArrayEquals(expected, result)

    sc.stop()


  }

}
