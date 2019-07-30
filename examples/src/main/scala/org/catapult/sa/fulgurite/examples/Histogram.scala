package org.catapult.sa.fulgurite.examples

import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Arguments, GeoSparkUtils}

/**
  * generate a histogram from a GeoTIFF.
  */
object Histogram extends Arguments {
  def main(args: Array[String]): Unit = {
    val opts = processArgs(args)
    val sc = getSparkContext("Example-histogram", "local[2]")
    val metaData = GeoTiffMeta(opts("input")) // read the metadata for the image.

    // read GeoTIFF, convert index->data to band, data, 1 then add them up.
    GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
      .filter { case (_, d) => d > 0 }
      .map { case (i, d) => (i.band -> d) -> 1 }
      .reduceByKey(_ + _)
      .collect() // now bring them back to the master and print them out. This should be ok, bands * 256 ish entries
      .foreach { case ((band, value), count) => println(band + "," + value + "," + count) }

    sc.stop() // stop the spark context now we are done with it.
  }

  override def allowedArgs() = List(InputArgument)
}
