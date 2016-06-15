package org.catapult.sa.geoprocess

import java.util.Date

import org.apache.spark.SparkContext
import org.catapult.sa.geotiff.GeoTiffMeta
import org.catapult.sa.spark._

import scala.collection.mutable

/**
  * Read a tiff and process it.
  */
object GeoTiffToASC extends SparkApplication {

  def main(args : Array[String]) : Unit = {

    case class Point(y : Long, x : Long) extends Ordered[Point] {
      import scala.math.Ordered.orderingToOrdered
      def compare(that: Point): Int = (this.y, this.x) compare (that.y, that.x)
    }

    val conf = configure(args)
    val sc = new SparkContext(conf)

    val (metaData, _) = GeoTiffMeta(opts("input"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc)
        .map { case (i, d) => Point(i.y, i.x) -> (i.band -> d)}
        .aggregateByKey(mutable.Buffer.empty[(Int, Int)], 1000)(SparkUtils.append, SparkUtils.appendAll)
        .sortBy(_._1)
        .map { case(p, d) =>
          val b = d.sortBy(_._1).toList
          val result = b.head.toString + " " + b.apply(1).toString + " " + b.apply(2).toString
          if (p.x == 0 && p.y == 0) {
            result
          } else if (p.x == 0) {
            "\n" + result
          } else {
            " " + result
          }
        }

    SparkUtils.saveMultiLineTextFile(converted, opts("output"))
    SparkUtils.joinOutputFiles("", opts("output"), "part", opts("output") + "output.asc")
    sc.stop()
  }

  override def extraArgs(): List[Argument] = List(Argument("input"), Argument("output"))

  override def defaultExtraArgs(): Map[String, String] = Map(
    "input" -> "c:/data/will/16April2016_Belfast_RGB_1.tif",
    "output" -> ("c:/data/will/test_" + new Date().getTime.toString + ".asc")
  )
}
