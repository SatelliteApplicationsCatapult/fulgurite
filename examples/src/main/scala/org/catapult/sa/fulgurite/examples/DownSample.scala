package org.catapult.sa.fulgurite.examples

import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils}

/**
  * This example takes an input GeoTIFF and makes it a factor smaller. Averaging all the pixels merged to make a
  * lower resolution resulting image.
  */
object DownSample extends Arguments {

  def main(args : Array[String]) : Unit = {

    // set up config and create spark context
    val opts = processArgs(args)
    val sc = getSparkContext("Example-DownSample", "local[1]")

    val partitionSize = opts("partitions").toLong // number of spark partitions to use.
    val sampleSize = opts("group").toInt // number of pixels to group together in each axis.

    // read the input image metadata
    val metaData = GeoTiffMeta(opts("input"))

    // read the GeoTIFF and group the indexes up to smaller values. Average the values for each band.
    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, partitionSize)
        .map { case (i, d) => i.groupFunction(sampleSize) -> d }
        .aggregateByKey(0 -> 0, 1000)(average, averageSum)
        .map(finalAverage)

    // create new metadata for the shrunk image.
    val newMeta = GeoTiffMeta(metaData)
    newMeta.width = metaData.width / sampleSize
    newMeta.height = metaData.height / sampleSize
    newMeta.pixelScales = metaData.pixelScales.map(_ * sampleSize)

    // save the result
    GeoSparkUtils.saveGeoTiff(converted, newMeta, opts("output"))
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    // close spark context as we are done with it.
    sc.stop()

    println(opts("output")) // Print where the output directory was so its easier to find it.
  }

  override def allowedArgs() = InputOutputArguments ++ List(
    Argument("group", "2"),
    Argument("partitions", "10")
  )

  def average(a : (Int, Int), b : Int) : (Int, Int) = (a._1 + b) -> (a._2 + 1)
  def averageSum(a : (Int, Int), b : (Int, Int)) : (Int, Int) = (a._1 + b._1) -> (a._2 + b._2)

  def finalAverage[T](d: (T, (Int, Int))) : (T, Int) = d._1 -> (d._2._1 / d._2._2)

}
