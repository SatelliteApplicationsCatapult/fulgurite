package org.catapult.sa.fulgurite.geotiff

/**
  * Index contains the location in the image of a pixel
  */
case class Index(x : Long, y : Long, band : Int) {
  def groupFunction(size : Int) : Index = {
    val newX = this.x / size
    val newY = this.y / size
    Index(newX, newY, this.band)
  }
}

object Index {

  def create(i : Long, width : Long, bandLength : Long, numBands : Int) : Index = {
    val band = i / bandLength
    val bandIndex = i - (bandLength * band)
    val x = bandIndex % width
    val y = bandIndex / width
    Index(x, y, band.asInstanceOf[Int])
  }

  def orderingByBandOutput[A <: Index] : Ordering[Index] = Ordering.by(i => (i.band, i.y, i.x))

  def orderingByPositionThenBand[A <: Index] : Ordering[Index] = Ordering.by(i => (i.y, i.x, i.band))

  // TODO: proximity curve ordering.

}
