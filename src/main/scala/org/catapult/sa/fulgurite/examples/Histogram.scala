package org.catapult.sa.fulgurite.examples

import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Arguments, GeoSparkUtils}

/**
  * generate a histogram from an image.
  */
object Histogram extends Arguments {
  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args)
    val sc = getSparkContext("Example-histogram", "local[2]")
    val metaData = GeoTiffMeta(opts("input"))

    GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .map { case (i, d) => (i.band -> d) -> 1 }
        .reduceByKey( _ + _ )
        .collect()
        .foreach { case ((band, value), count) => println(band + "," + value + "," + count) }

    sc.stop()
  }

  override def allowedArgs() = List(InputArgument)
}
