package org.catapult.sa.fulgurite.examples

import java.io.{FileOutputStream, PrintStream}
import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * Read a tiff and turn it into an ASC file
  */
object GeoTiffToASC extends Arguments {

  def main(args : Array[String]) : Unit = {

    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Convert", "local[2]")
    val sc = SparkContext.getOrCreate(conf)

    val (metaData, _) = GeoTiffMeta(opts("input"))
    val targetBand = opts("band").toInt
    if (metaData.samplesPerPixel <= targetBand) {
      throw new IllegalArgumentException("band must be less than the number of bands in the image.")
    }

    // pick the ordering we want for the output.
    implicit val indexOrder = Index.orderingByBandOutput

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .filter { case (i, d) => i.band == targetBand }
        .sortByKey()
        .map { case(i, d) =>
          if (i.x == 0 && i.y == 0) {
            d.toString
          } else if (i.x == 0) {
            "\n" + d.toString
          } else {
            " " + d.toString
          }
        }

    SparkUtils.saveRawTextFile(converted, opts("output"))
    generateHeader(metaData, opts("output") + "/header.txt")
    SparkUtils.joinOutputFiles(opts("output") + "/header.txt", opts("output"), opts("output") + "/output.asc")
    sc.stop()
  }

  override def allArgs(): List[Argument] = List("input", "output", "band")

  override def defaultArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".asc"),
    "band" -> "0"
  )

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
