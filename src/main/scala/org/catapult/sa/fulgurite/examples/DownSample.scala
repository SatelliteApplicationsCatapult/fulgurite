package org.catapult.sa.fulgurite.examples

import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * make the input geotiff some factor smaller
  */
object DownSample extends Arguments {

  def main(args : Array[String]) : Unit = {

    // set up config and create spark context
    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Convert", "local[1]")
    val sc = SparkContext.getOrCreate(conf)

    val partitionSize = opts("partitions").toLong
    val sampleSize = opts("group").toInt

    // read the input image metadata
    val (metaData, rawMeta) = GeoTiffMeta(opts("input"))

    // read the geoTiff and group the indexes up to smaller values. Average the values for each band.
    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, partitionSize)
        .map { case (i, d) => i.groupFunction(sampleSize) -> d }
        .aggregateByKey(0 -> 0, 1000)(SparkUtils.average, SparkUtils.averageSum)
        .map(SparkUtils.finalAverage)

    // create new metadata for the shrunk image.
    val newMeta = GeoTiffMeta(metaData)
    newMeta.width = metaData.width / sampleSize
    newMeta.height = metaData.height / sampleSize
    newMeta.pixelScales = metaData.pixelScales.map(_ * sampleSize)

    // save the result
    GeoSparkUtils.saveGeoTiff(converted, newMeta, rawMeta, opts("output"))
    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    // close spark context as we are done with it.
    sc.stop()

    println(opts("output")) // Print where the output directory was so its easier to find it.
  }

  override def allowedArgs() = List(
    Argument("input", "src/test/resources/tiny.tif"),
    Argument("output", FileUtils.getTempDirectoryPath + "/test_" + new Date().getTime.toString + ".tif"),
    Argument("group", "2"),
    Argument("partitions", "10")
  )
}
