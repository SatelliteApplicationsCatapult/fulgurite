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
    val sc = getSparkContext("Example-BW", "local[3]")
    val metaData = GeoTiffMeta(opts("input"))
    val inut = "input"
      println(inut)

    if (metaData.samplesPerPixel == 1) {
      println("requires a GeoTIFF with more than one band of data")
    }

    val input = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, 4000000)
      .filter(e => e._1.band==2 || e._1.band==3)//assuming e starts from 0, getting bands 3 and 4 from the array, prod
      //.collect().foreach(println)//how do you print without changing the input so it can be placed into teh next row
    val blackAndWhite = GeoSparkUtils.pixelGroup(input, metaData.samplesPerPixel)
      .map { case (i, d) =>
        val result = if ((d(2).get + d(3).get) == 0) {
          0
        } else {
          (((d(3).get.toDouble - d(2).get.toDouble) / (d(2).get.toDouble + d(3).get.toDouble))*256.0).toInt
        }
        Index.apply(i._1, i._2, 0) -> result
      }

    val resultMeta = GeoTiffMeta(metaData)
    resultMeta.samplesPerPixel = 1
    resultMeta.photometricInterpretation = BaselineTIFFTagSet.PHOTOMETRIC_INTERPRETATION_BLACK_IS_ZERO
    resultMeta.extraSamples = Array(0)
    resultMeta.sampleFormat = Array(BaselineTIFFTagSet.SAMPLE_FORMAT_UNSIGNED_INTEGER)
    resultMeta.bitsPerSample = Array(16)


    GeoSparkUtils.saveGeoTiff(blackAndWhite, resultMeta, opts("output"))
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()
    println(opts("output"))
  }
}
