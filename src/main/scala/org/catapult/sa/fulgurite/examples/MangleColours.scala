package org.catapult.sa.fulgurite.examples

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}
import org.catapult.sa.spark._

object MangleColours extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args, defaultArgs())
    val conf = SparkUtils.createConfig("Example-Red", "local[2]")
    val sc = new SparkContext(conf)

    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
     /*.map { case (i, d) =>
        i.band match {
          case 0 => i -> 255
          case _ => i -> d
        }
     }*/

    GeoSparkUtils.saveGeoTiff(converted, metaData, baseMeta, opts("output"))

    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), "part-", opts("output") + "/data.tif")

    sc.stop()

    //SparkUtils.deleteAllExcept(opts("output"), "data.tif")
  }

  override def allArgs(): List[Argument] = List("input", "output")

  override def defaultArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",

    //"input" -> "C:/data/Will/OUREA_SiteB_24102915_WV_processedImg_mk2.tif",
    //"input" -> "C:/Users/Wil.Selwood/Downloads/S1A_IW_SLC__1SDV_20160610T175738_20160610T175806_011652_011D4B_9469.SAFE/measurement/s1a-iw1-slc-vh-20160610t175739-20160610t175804-011652-011d4b-001.tiff",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif")
  )

}
