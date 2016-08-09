package org.catapult.sa.fulgurite.examples

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils}

/**
  * Simple example converting a three colour GeoTIFF into a single band black and white image.
  */
object BlackAndWhite extends Arguments {
  override def allowedArgs(): List[Argument] = InputOutputArguments

  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args)
    val sc = getSparkContext("Example-BW", "local[3]")
    val metaData = GeoTiffMeta(opts("input"))

    if (metaData.samplesPerPixel == 1) {
      println("requires a GeoTIFF with more than one band of data")
    }

    val input = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, 400000)
    val blackAndWhite = GeoSparkUtils.pixelGroup(input, metaData.samplesPerPixel)
      .map { case (i, d) =>
        val values = d.filter(_.isDefined).map(_.get)
        Index(i._1, i._2, 0) ->  values.sum / values.length
      }

    val resultMeta = GeoTiffMeta(metaData)
    resultMeta.samplesPerPixel = 1
    resultMeta.photometricInterpretation = BaselineTIFFTagSet.PHOTOMETRIC_INTERPRETATION_BLACK_IS_ZERO

    GeoSparkUtils.saveGeoTiff(blackAndWhite, resultMeta, opts("output"))
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()
    println(opts("output"))
  }
}
