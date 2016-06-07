package org.catapult.sa.geotiff

/**
  * Created by Wil.Selwood on 31/05/2016.
  */
case class Index(x : Long, y : Long, i : Long)

object Index {
  def apply(i : Long, meta : GeoTiffMeta) : Index = {
    apply(i, meta.width)
  }

  def apply(i : Long, width : Long) : Index = {

    val x = Math.floorMod(i, width)
    val y = Math.floorDiv(i, width)

    Index(x, y, i)
  }
}
