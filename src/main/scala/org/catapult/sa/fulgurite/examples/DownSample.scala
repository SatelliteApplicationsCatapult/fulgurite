package org.catapult.sa.fulgurite.examples

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * make the input geotiff some factor smaller
  */
object DownSample extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Convert", "local[1]")
    val sc = new SparkContext(conf)

    val (metaData, rawMeta) = GeoTiffMeta(opts("input"))

    val partitionSize = opts("partitions").toLong

    val sampleSize = opts("group").toInt

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, partitionSize)
        .map { case (i, d) => i.groupFunction(sampleSize) -> d }
        .aggregateByKey(0 -> 0, 1000)(SparkUtils.average, SparkUtils.averageSum)
        .map(SparkUtils.finalAverage)

    val newMeta = GeoTiffMeta(metaData.width / sampleSize, metaData.height / sampleSize, metaData.samplesPerPixel, metaData.bitsPerSample, 0, 0, metaData.tiePoints, metaData.pixelScales.map(_ * sampleSize), metaData.colourMode, metaData.planarConfiguration)

    GeoSparkUtils.saveGeoTiff(converted, newMeta, rawMeta, opts("output"))

    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")
    sc.stop()
  }

  override def allArgs(): List[Argument] = List("input", "output", "group", "partitions")

  override def defaultArgs(): Map[String, String] = Map(
    "input" -> "C:/data/OUREA_SiteB_24102915_WV_processedImg-cropped.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif"),
    "group" -> "2",
    "partitions" -> "400000"
  )
}
