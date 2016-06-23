package org.catapult.sa.geotiff

/**
  * Index contains the location in the image of a pixel
  */
case class Index(x : Long, y : Long, band : Int) {
  def groupFunction(size : Int) : Index = {
    val x = this.x / size
    val y = this.y / size
    Index(x, y, this.band)
  }
}

object Index {

  // not called apply as it has the same signature as the built in version of apply for the case class.
  def create(i : Long, width : Long, bandLength : Long, numBands : Int) : Index = {
    val band = i / bandLength
    val bandIndex = i / numBands
    Index(Math.floorMod(bandIndex, width), Math.floorDiv(bandIndex, width), band.asInstanceOf[Int])
  }

  def orderingByBandOutput[A <: Index] : Ordering[Index] = Ordering.by(i => (i.band, i.y, i.x))

  def orderingByPositionThenBand[A <: Index] : Ordering[Index] = Ordering.by(i => (i.y, i.x, i.band))

  // TODO: proximity curve ordering.

}
