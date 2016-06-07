package org.catapult.sa.geotiff

/**
  * a data point from a geotiff file.
  */
trait DataPoint {
  val r : Int
  val g : Int
  val b : Int
  val a : Int
}

case class RGBDataPoint(r : Int, g : Int, b : Int) extends DataPoint {
  override val a: Int = 255
}
case class RGBADataPoint(r : Int, g : Int, b : Int, a : Int) extends DataPoint

object DataPoint {
  def apply(r : Byte, g : Byte, b : Byte) : DataPoint = {
    // handle byte conversion here. We don't want the result to be signed so not using normal .toInt
    RGBDataPoint(r & 0xFF, g & 0xFF, b & 0xFF)
  }

  def apply(b : Array[Byte], meta : GeoTiffMeta) : DataPoint = {
    buildConverter(meta : GeoTiffMeta)(b)
  }

  def buildConverter(meta : GeoTiffMeta) : (Array[Byte]) => DataPoint = {
    meta.samplesPerPixel match {
      case 3 =>
        if (meta.bitsPerSample(0) == 8 && meta.bitsPerSample(1) == 8  && meta.bitsPerSample(1) == 8) {
          (b : Array[Byte]) => RGBDataPoint(b(0) & 0xFF, b(1) & 0xFF, b(2) & 0xFF)
        } else {
          throw new IllegalArgumentException("Non 8bit files are not yet supported. Find this error and implement please")
        }
      case _ => throw new IllegalArgumentException("Non rgb files are not yet supported. Find this error and implement please")
    }
  }
}

