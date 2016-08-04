package org.catapult.sa.fulgurite.examples

import java.io.{FileOutputStream, PrintStream}

import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils}

/**
  * Read a GeoTIFF and turn it into an ASC file
  */
object GeoTiffToASC extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val sc = getSparkContext("Example-Convert", "local[2]")

    // get the metadata and make sure that the target band is actually in the image.
    val metaData = GeoTiffMeta(opts("input"))
    val targetBand = opts("band").toInt
    if (metaData.samplesPerPixel <= targetBand) {
      throw new IllegalArgumentException("band must be less than the number of bands in the image.")
    }

    // pick the ordering we want for the output.
    implicit val indexOrder = Index.orderingByBandOutput

    // read the GeoTIFF, filter, sort and convert into ascii format.
    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .filter { case (i, d) => i.band == targetBand } // only pull out the band we are interested in.
        .sortByKey() // this uses the implicit ordering we declared earlier
        .map { case(i, d) =>
          (i.x, i.y) match {
            case (0, 0) => d.toString
            case (0, _) => "\n" + d.toString
            case (_, _) => " " + d.toString
          }
        }

    // save the result as a raw text file.
    GeoSparkUtils.saveRawTextFile(converted, opts("output"))
    generateHeader(metaData, opts("output") + "/header.txt")
    GeoSparkUtils.joinOutputFiles(opts("output") + "/header.txt", opts("output"), opts("output") + "/output.asc")
    sc.stop() // stop the context now we are finished with it.

    println(opts("output")) // Print where the output directory was so its easier to find it.
  }

  override def allowedArgs() = Argument("band", "0") :: InputOutputArguments

  private def generateHeader(meta : GeoTiffMeta, target : String): Unit = {
    val output = new PrintStream(new FileOutputStream(target))
    generateHeader(meta, output)
    output.close()
  }

  private def generateHeader(meta : GeoTiffMeta, output : PrintStream) : Unit = {
    output.print("ncols        ")
    output.println(meta.width)
    output.print("nrows        ")
    output.println(meta.height)

    if (meta.tiePoints != null && meta.tiePoints.length > 2) {
      val points = meta.tiePoints.filter(_ > 0.0)

      output.print("xllcorner    ")
      output.println(points.head)

      output.print("yllcorner    ")
      output.println(points.last)
    }

    output.print("cellsize     ")
    output.println(meta.pixelScales.head)
  }
}
