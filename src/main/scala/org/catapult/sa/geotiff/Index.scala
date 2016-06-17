package org.catapult.sa.geotiff

/**
  * Index contains the location in the image of a pixel
  */
case class Index(x : Long, y : Long, i : Long, band : Int)

object Index {
  def apply(i : Long, meta : GeoTiffMeta) : Index = {
    create(i, meta.width, meta.height, meta.samplesPerPixel)
  }

  // not called apply as it has the same signature as the built in version of apply for the case class.
  def create(i : Long, width : Long, bandLength : Long, numBands : Int) : Index = {
    val band = i / bandLength
    val bandIndex = i / numBands
    Index(Math.floorMod(bandIndex, width), Math.floorDiv(bandIndex, width), i, band.asInstanceOf[Int])
  }
}
