package org.catapult.sa.fulgurite.examples

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils}

/**
  * Simple example converting a three colour GeoTIFF into a single band black and white image.
  */
object NDVI extends Arguments {
  override def allowedArgs(): List[Argument] = InputOutputArguments

  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args)
    val sc = getSparkContext("Example-NDVI", "local[3]")
    val metaData = GeoTiffMeta(opts("input"))
    val inut = "input"
      println(inut)

    if (metaData.samplesPerPixel == 1) {
      println("requires a GeoTIFF with more than one band of data")
    }

    val NIR_Band = 3
    val Red_Band = 2
    val input = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, 4000000)
      .filter(e => e._1.band==Red_Band || e._1.band==NIR_Band)
    val NDVI = GeoSparkUtils.pixelGroup(input, metaData.samplesPerPixel)
      .map { case (i, d) =>
        val NIR = d(NIR_Band).get.toDouble
        val Red = d(Red_Band).get.toDouble
        val result = if ((NIR + Red) == 0) {
          0
        } else {
          (((NIR - Red) / (NIR + Red))*256.0).toInt
        }
        Index.apply(i._1, i._2, 0) -> result
      }

    val resultMeta = GeoTiffMeta(metaData)
    resultMeta.samplesPerPixel = 1
    resultMeta.photometricInterpretation = BaselineTIFFTagSet.PHOTOMETRIC_INTERPRETATION_BLACK_IS_ZERO
    resultMeta.extraSamples = Array(0)
    resultMeta.sampleFormat = Array(BaselineTIFFTagSet.SAMPLE_FORMAT_SIGNED_INTEGER)
    resultMeta.bitsPerSample = Array(16)


    GeoSparkUtils.saveGeoTiff(NDVI, resultMeta, opts("output"))
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()
    println(opts("output"))
  }
}
