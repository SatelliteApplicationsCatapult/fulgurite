package org.catapult.sa.fulgurite.examples

import java.util.Date

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel}
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}
import org.catapult.sa.fulgurite.spark.{Argument, Arguments, GeoSparkUtils, SparkUtils}

/**
  * Code to add a transparent layer and make part of the image transparent.
  */
object MakeAreaTransparent extends Arguments {

  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args)
    val conf = SparkUtils.createConfig("Example-Transparent", "local[4]")
    val sc = SparkContext.getOrCreate(conf)

    val (metaData, baseMeta) = GeoTiffMeta(opts("input"))

    val targetBand =  metaData.samplesPerPixel

    val area = parseShape(opts("shape"))

    val converted = GeoSparkUtils.GeoTiffRDD(opts("input"), metaData, sc, 10)
        .flatMap { case (index, value) => // for each
          val transparentIndex = Index(index.x, index.y, targetBand)
          val point = index.toPoint(geoFactory)

          // Only create the extra band for one of the original bands. So we don't end up with duplicates.
          if (index.band == 0) {
            // if the point is in side the area then it is visible otherwise it is transparent
            if (area.contains(point)) {
              List(index -> value, transparentIndex -> 255)
            } else {
              List(index -> value, transparentIndex -> 0)
            }
          } else {
            List(index -> value)
          }
        }

    // create new metadata with extra transparent band
    val resultMeta = GeoTiffMeta(metaData)
    resultMeta.samplesPerPixel = metaData.samplesPerPixel + 1 // One extra sample
    resultMeta.bitsPerSample = (8 :: metaData.bitsPerSample.toList).toArray // that is 8 bits
    resultMeta.extraSamples  = (BaselineTIFFTagSet.EXTRA_SAMPLES_UNASSOCIATED_ALPHA :: metaData.extraSamples.toList).toArray  // the extra sample is a alpha channel
    resultMeta.sampleFormat  = (BaselineTIFFTagSet.SAMPLE_FORMAT_UNSIGNED_INTEGER :: metaData.sampleFormat.toList).toArray  // the extra sample is an unsigned int.

    GeoSparkUtils.saveGeoTiff(converted, resultMeta, baseMeta, opts("output"))
    SparkUtils.joinOutputFiles(opts("output") + "/header.tiff", opts("output"), opts("output") + "/data.tif")

    sc.stop()

    println(opts("output"))
  }

  override def allowedArgs() = List(
    Argument("input", "src/test/resources/tiny.tif"),
    Argument("output", FileUtils.getTempDirectoryPath + "/test_" + new Date().getTime.toString + ".tif"),
    Argument("shape", "5:0,11:5,5:11,0:5,5:0")
  )

  private def parseShape(input : String) : Polygon = {
    geoFactory.createPolygon(input.split(",").map { i =>
      val parts = i.split(":")
      new Coordinate(parts(0).toDouble, parts(1).toDouble)
    })
  }

  private lazy val geoFactory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED))
}
