package org.catapult.sa.geoprocess

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.geotiff.GeoTiffMeta
import org.catapult.sa.spark._

/**
  * Read a tiff and process it.
  */
object GeoTiffToASC extends SparkApplication {

  def main(args : Array[String]) : Unit = {

    val conf = configure(args)
    val sc = new SparkContext(conf)

    val (metaData, _) = GeoTiffMeta(opts("input"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
      .sortBy(_._1.i)
      .map (e => {
        val result = e._2.r.toString + " " + e._2.g.toString + " " + e._2.b.toString
        if (e._1.x == 0 && e._1.y == 0) {
          result
        } else if (e._1.x == 0) {
          "\n" + result
        } else {
          " " + result
        }
      })

    saveMultiLineTextFile(converted, opts("output"))
    sc.stop()
  }

  override def extraArgs(): List[Argument] = List(Argument("input"), Argument("output"))

  override def defaultExtraArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".asc")
  )
}
