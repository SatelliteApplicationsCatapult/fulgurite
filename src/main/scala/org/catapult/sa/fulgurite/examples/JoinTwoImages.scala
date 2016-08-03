package org.catapult.sa.fulgurite.examples

import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils}

/**
  * Example program to stitch two GeoTiff images together.
  */
object JoinTwoImages extends Arguments  {


  def main(args : Array[String]) : Unit = {

    // Create spark context and process arguments
    val opts = processArgs(args)
    val sc = getSparkContext("Example-Transparent", "local[3]")

    // get the metadata for the input images
    val metaData1 = GeoTiffMeta(opts("input1"))
    val metaData2 = GeoTiffMeta(opts("input2"))

    // read the first one NOTE: you will need to change the partition size for your images.
    // this is something that will need changing for your images and cluster
    val image1 = GeoSparkUtils.GeoTiffRDD(opts("input1"), metaData1, sc, 10)

    // offset the second image by the width of the first so it ends up on the right of the original
    val image2 = GeoSparkUtils.GeoTiffRDD(opts("input2"), metaData2, sc, 10)
      .map { case (index, value) => Index(index.x + metaData1.width, index.y, index.band) -> value }

    // if the two images have different heights then we need to fill in the space at the bottom on one of them with black
    // The other option is to filter one of the RDDs so that they are both the same height.
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

    // join the three bits up
    val result = image1.union(image2).union(fillRDD)

    // generate the new metadata for the resulting image.
    val resultMeta = GeoTiffMeta(metaData1)
    resultMeta.width = metaData1.width + metaData2.width
    resultMeta.height = Math.max(metaData1.height, metaData2.height)
    // NOTE: if we were placing the new image on the left of the first one we would have to update the tie points here.

    // write the result
    GeoSparkUtils.saveGeoTiff(result, resultMeta, opts("output"))
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    // we are now finished with the spark context so stop it.
    sc.stop()

    println(opts("output"))
  }

  override def allowedArgs() = List(
    Argument("input1", "src/test/resources/tiny.tif"),
    Argument("input2", "src/test/resources/tiny.tif"),
    OutputArgument
  )
}
