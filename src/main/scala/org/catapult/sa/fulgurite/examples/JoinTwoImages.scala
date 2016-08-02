package org.catapult.sa.fulgurite.examples

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * Example program to stitch two GeoTiff images together.
  */
object JoinTwoImages extends Arguments  {


  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Transparent", "local[3]")
    val sc = SparkContext.getOrCreate(conf)

    val (metaData1, baseMeta1) = GeoTiffMeta(opts("input1"))
    val (metaData2, _) = GeoTiffMeta(opts("input2"))

    val image1 = GeoSparkUtils.GeoTiffRDD(opts("input1"), metaData1, sc, 10000)

    // offset the second image by the width of the first so it ends up on the right of the original
    val image2 = GeoSparkUtils.GeoTiffRDD(opts("input2"), metaData2, sc, 10000)
      .map { case (index, value) => Index(index.x + metaData1.width, index.y, index.band) -> value }

    // if the two images have different heights then we need to fill in the space at the bottom on one of them with black
    val fillRDD = if (metaData1.height > metaData2.height) {
      sc.parallelize(for (y <- metaData2.height until metaData1.height;
                          x <- metaData1.width until metaData2.width + metaData1.width;
                          band <- 0 until metaData1.samplesPerPixel)
                      yield Index(x, y, band) -> 0)
    } else if (metaData1.height < metaData2.height) {
      sc.parallelize(for (y <- metaData1.height until metaData2.height;
                          x <- 0L until metaData1.width;
                          band <- 0 until metaData1.samplesPerPixel)
                    yield Index(x, y, band) -> 0)
    } else {
      sc.emptyRDD[(Index, Int)]
    }


    val result = image1.union(image2).union(fillRDD)

    val resultMeta = GeoTiffMeta(metaData1)
    resultMeta.width = metaData1.width + metaData2.width
    resultMeta.height = Math.max(metaData1.height, metaData2.height)
    // NOTE: if we were placing the new image on the left of the first one we would have to update the tie points here.

    GeoSparkUtils.saveGeoTiff(result, resultMeta, baseMeta1, opts("output"))
    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()

  }

  override def allArgs(): List[Argument] = List("input1", "input2", "output")

  override def defaultArgs(): Map[String, String] = Map(
    "input1" -> "c:/data/Will/16April2016_Belfast_RGB_1.tif",
    "input2" -> "c:/data/Will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/Will/test_" + new Date().getTime.toString + ".tif")
  )
}
