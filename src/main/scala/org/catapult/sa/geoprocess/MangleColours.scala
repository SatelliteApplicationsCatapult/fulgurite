package org.catapult.sa.geoprocess

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.geotiff.GeoTiffMeta
import org.catapult.sa.spark.{Argument, GeoSparkUtils, SparkApplication, SparkUtils}

object MangleColours extends SparkApplication {

  def main(args : Array[String]) : Unit = {

    val conf = configure(args)

    val sc = new SparkContext(conf)

    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    println(metaData)

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
     .map { case (i, d) =>
        i.band match {
          case 0 => i -> 255
          case _ => i -> d
        }
     }

    GeoSparkUtils.saveGeoTiff(converted, metaData, baseMeta, opts("output"))

    println("Joining up output files...")
    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), "part-", opts("output") + "/data.tif")

    sc.stop()

    //SparkUtils.deleteAllExcept(opts("output"), "data.tif")
  }

  override def extraArgs(): List[Argument] = List(Argument("input"), Argument("output"))

  override def defaultExtraArgs(): Map[String, String] = Map(
    "input" -> "C:/data/S1_IW_GRDH_1SDV_20150204_T17_5710/S1_IW_GRDH_1SDV_20150204_T17_5710_ML2_ELL_CAL_GAMMA0_VH_TC_SRTM90_WGS84LL_AOI_Spk_Mean55.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".tif")
  )

}
