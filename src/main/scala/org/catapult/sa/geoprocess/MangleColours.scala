package org.catapult.sa.geoprocess

import java.io.File
import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.geotiff.{GeoTiffMeta, RGBDataPoint}
import org.catapult.sa.spark.{Argument, GeoSparkUtils, SparkApplication}

object MangleColours extends SparkApplication {

  def main(args : Array[String]) : Unit = {

    val conf = configure(args)
    val sc = new SparkContext(conf)

    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    println(metaData)

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
     /* .map { case (i, d) =>
        val max = Math.max(Math.max(d.r, d.g), d.b)

        val r = if (d.r == max) { 255 } else { 0 }
        val g = if (d.g == max) { 255 } else { 0 }
        val b = if (d.b == max) { 255 } else { 0 }

        i -> RGBDataPoint(r, g, b)
      }*/

    GeoSparkUtils.saveGeoTiff(converted, metaData, baseMeta, opts("output"))

    println("Joining up output files...")
    joinOutputFiles(opts("output"), "part-", opts("output") + "/data.tif")
    println("Adding header...")
    joinFiles(opts("output") + "/result.tif", new File(opts("output") + "/header.tiff"), new File(opts("output") + "/data.tif"))

    sc.stop()
  }

  override def extraArgs(): List[Argument] = List(Argument("input"), Argument("output"))

  override def defaultExtraArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif")
  )

}
