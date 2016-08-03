package org.catapult.sa.fulgurite.examples

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Arguments, GeoSparkUtils}

object MangleColours extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val sc = getSparkContext("Example-Orange", "local[2]")

    val metaData = GeoTiffMeta(opts("input"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, 10)
      .map { case (i, d) =>
        i.band match {
          case 0 => i -> 255
          case 1 => i -> 128
          case 2 => i -> 0
          case _ => i -> d
        }
      }

    GeoSparkUtils.saveGeoTiff(converted, metaData, opts("output"), 10)
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()
    println(opts("output"))
  }

  override def allowedArgs() = InputOutputArguments

}
