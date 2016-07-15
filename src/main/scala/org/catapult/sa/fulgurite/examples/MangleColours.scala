package org.catapult.sa.fulgurite.examples

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

object MangleColours extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Red", "local[2]")
    val sc = new SparkContext(conf)

    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
      .map { case (i, d) =>
        i.band match {
          case 0 => i -> 255
          case _ => i -> d
        }
      }

    GeoSparkUtils.saveGeoTiff(converted, metaData, baseMeta, opts("output"))
    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()
  }

  override def allArgs(): List[Argument] = List("input", "output")

  override def defaultArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif")
  )

}
