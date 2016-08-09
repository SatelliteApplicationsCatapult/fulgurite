package org.catapult.sa.fulgurite.integration

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.GeoSparkUtils
import org.junit.Assert._
import org.junit.Test

/**
  * Simple tests of the grouping functions
  */
class TestGrouping {

  @Test
  def group() : Unit = {
    val sc = getSparkContext("TestGrouping", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_NONE,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    val input = sc.parallelize(
      (0 until 11).flatMap(y => (0 until 11).flatMap(x =>
        List(Index(x, y, 0) -> (x*y), Index(x, y, 1) -> (x+y), Index(x, y, 2) -> (x-y))
      ))
    )

    val result = GeoSparkUtils.pixelGroup(input, metaData.samplesPerPixel).collect().toList.sortBy(_._1)

    val expected = (0 until 11).flatMap(x => (0 until 11).map(y => (x.toLong -> y.toLong) -> Array(Option(x*y),Option(x+y), Option(x-y)))).toList

    assertEquals(expected.length, result.length)
    assertEquals(expected.map(_._1), result.map(_._1))

    expected.zip(result).zipWithIndex.foreach(p => assertArrayEquals("failed cell " + p._2, p._1._1._2.map(_.getOrElse(-1)), p._1._2._2.map(_.getOrElse(-1))))

  }

  @Test
  def groupMissingBands() : Unit = {
    val sc = getSparkContext("TestGrouping", "local[2]")

    val metaData = GeoTiffMeta(11L, 11L, 3, Array(8, 8, 8), 0, 0, Array(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      Array(1.2, 1.2, 0.0), 2, 1, Array.empty[Int], Array(1, 1, 1), "WGS 84 / UTM zone 30N|WGS 84|",
      Array(1L, 1L), Array(1L, 1L), BaselineTIFFTagSet.COMPRESSION_NONE,
      Array(1, 1, 0, 7, 1024, 0, 1, 1, 1025, 0, 1, 1, 1026, 34737, 7, 22, 2054, 0, 1, 9102, 3072, 0, 1, 32630, 3076, 0, 1, 9001)
    )

    val input = sc.parallelize(
      (0 until 11).flatMap(y => (0 until 11).flatMap(x =>
        (if (x != 5 || y != 5) List(Index(x, y, 2) -> (x-y)) else List()) ++ List(Index(x, y, 0) -> (x*y), Index(x, y, 1) -> (x+y)) )
      )
    )

    val result = GeoSparkUtils.pixelGroup(input, metaData.samplesPerPixel).collect().toList.sortBy(_._1)

    val expected = (0 until 11).flatMap(x => (0 until 11).map(y =>
      if (x != 5 || y != 5) (x.toLong -> y.toLong) -> Array(Some(x*y),Some(x+y), Some(x-y)) else (x.toLong -> y.toLong) -> Array(Some(25), Some(10), None)
    )).toList

    assertEquals(expected.length, result.length)
    assertEquals(expected.map(_._1), result.map(_._1))

    expected.zip(result).zipWithIndex.foreach(p => assertArrayEquals("failed cell " + p._2, p._1._1._2.map(_.getOrElse(-1)), p._1._2._2.map(_.getOrElse(-1))))
  }

  @Test
  def unGroup() : Unit = {
    val sc = getSparkContext("TestGrouping", "local[2]")

    val input = sc.parallelize((0 until 11).flatMap(x => (0 until 11).map(y => (x.toLong -> y.toLong) -> Array(Option(x*y), Option(x+y), Option(x-y)))))

    implicit val ordering = Index.orderingByPositionThenBand

    val result = GeoSparkUtils.pixelUnGroup(input).collect().toList.sortBy(_._1)
    val expected = (0 until 11).flatMap(y => (0 until 11).flatMap(x =>
      List(Index(x, y, 0) -> (x*y), Index(x, y, 1) -> (x+y), Index(x, y, 2) -> (x-y))
    )).toList.sortBy(_._1)

    assertEquals(expected.length, result.length)
    assertEquals(expected.map(_._1), result.map(_._1))

    expected.zip(result).zipWithIndex.foreach(p => assertEquals("failed cell " + p._2, p._1._1._2, p._1._2._2))

  }

}
