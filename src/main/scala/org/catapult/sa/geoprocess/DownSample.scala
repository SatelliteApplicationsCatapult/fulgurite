package org.catapult.sa.geoprocess

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.geotiff.GeoTiffMeta
import org.catapult.sa.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * make the input geotiff some factor smaller
  */
object DownSample extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args, defaultArgs())
    val conf = SparkUtils.createConfig("Example-Convert", "local[2]")
    val sc = new SparkContext(conf)

    val (metaData, rawMeta) = GeoTiffMeta(opts("input"))

    val sampleSize = opts("group").toInt

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .map { case (i, d) => i.groupFunction(sampleSize) -> d }
        .groupByKey(100)
        .map { case (i, d) => i -> (d.sum / d.size) }

    val newMeta = GeoTiffMeta(metaData.width / sampleSize, metaData.height / sampleSize, metaData.samplesPerPixel, metaData.bitsPerSample, 0, 0, metaData.tiePoints, metaData.pixelScales)

    GeoSparkUtils.saveGeoTiff(converted, newMeta, rawMeta, opts("output"))

    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), "part-", opts("output") + "/data.tif")
    sc.stop()
  }

  override def allArgs(): List[Argument] = List("input", "output", "group")

  override def defaultArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif"),
    "group" -> "2"
  )
}
