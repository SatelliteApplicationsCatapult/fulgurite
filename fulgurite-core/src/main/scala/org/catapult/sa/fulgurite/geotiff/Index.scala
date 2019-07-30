package org.catapult.sa.fulgurite.geotiff

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}

/**
  * Index contains the location in the image of a pixel
  */
case class Index(x : Long, y : Long, band : Int) {
  def groupFunction(size : Int) : Index = {
    val newX = this.x / size
    val newY = this.y / size
    Index(newX, newY, this.band)
  }

  def toCoordinate = new Coordinate(x, y)

  def toPoint(geoFactory : GeometryFactory) : Point = {
    geoFactory.createPoint(this.toCoordinate)
  }

}

object Index {

  def createPlanar(i : Long, width : Long, bandLength : Long, numBands : Int) : Index = {
    val band = i / bandLength//RGBRGB...
    val bandIndex = i - (bandLength * band)
    val x = bandIndex % width
    val y = bandIndex / width//where its in the line of noms.
    Index(x, y, band.asInstanceOf[Int])
  }

  def createChunky(i : Long, width : Long, height : Long, numBands : Int) : Index = {
//reading the long line noms
    val band = i  % numBands// remainder of the which band we are in
    val x = (i / numBands) % height// x value we are in
    val y = (i / numBands) / height// y value we are in

    Index(x, y, band.asInstanceOf[Int])
  }

  def orderingByBandOutput[A <: Index] : Ordering[Index] = Ordering.by(i => (i.band, i.y, i.x))

  def orderingByPositionThenBand[A <: Index] : Ordering[Index] = Ordering.by(i => (i.y, i.x, i.band))

  // TODO: proximity curve ordering.
  // It would be useful / more performant to have an ordering where pixels near each other end up in the same partition.

}
