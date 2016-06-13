package org.catapult.sa.geotiff

/**
  * Index contains the location in the original image of a pixel
  */
case class Index(x : Long, y : Long, i : Long)

object Index {
  def apply(i : Long, meta : GeoTiffMeta) : Index = {
    apply(i, meta.width)
  }

  def apply(i : Long, width : Long) : Index = {
    Index(Math.floorMod(i, width), Math.floorDiv(i, width), i)
  }
}
