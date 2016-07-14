package org.catapult.sa.fulgurite.examples

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}
import org.catapult.sa.spark.GeoSparkUtils

/**
  * generate a histogram from an image.
  */
object Histogram extends Arguments {
  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args, defaultArgs())
    val conf = SparkUtils.createConfig("Example-histogram", "local[2]")
    val sc = new SparkContext(conf)
    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .map { case (i, d) => (i.band -> d) -> 1 }
        .reduceByKey( _ + _ )
        .collect()
        .foreach { case ((band, value), count) => println(band + "," + value + "," + count) }

    sc.stop()
  }

  override def allArgs(): List[Argument] = List("input")
  override def defaultArgs(): Map[String, String] = Map("input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif")
}
