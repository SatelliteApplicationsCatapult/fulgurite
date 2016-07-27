package org.catapult.sa.fulgurite.geotiff

import org.junit.Test
import org.junit.Assert._

class TestIndex {

  @Test
  def testGroupingFunction() : Unit = {
    val input = Index(4, 9, 1)
    val result = input.groupFunction(2)

    assertEquals(2, result.x)
    assertEquals(4, result.y)
    assertEquals(1, result.band)
  }

  @Test
  def testGroupingFunctionZero() : Unit = {
    val input = Index(0, 1, 1)
    val result = input.groupFunction(2)

    assertEquals(0, result.x)
    assertEquals(0, result.y)
    assertEquals(1, result.band)
  }

  @Test
  def testEqualsBands() : Unit = {
    val in1 = Index(0, 0, 1)
    val in2 = Index(0, 0, 2)

    assertFalse(in1.equals(in2))
  }


  @Test
  def testCreate() : Unit = {
    val result = Index.createPlanar(1, 11478, 11478 * 10782, 3)
    assertEquals(1, result.x)
    assertEquals(0, result.y)
    assertEquals(0, result.band)
  }

  @Test
  def testCreateSecondBand() : Unit = {
    val result = Index.createPlanar((11478 * 10782) + 1, 11478, 11478 * 10782, 3)
    assertEquals(1, result.x)
    assertEquals(0, result.y)
    assertEquals(1, result.band)
  }

  @Test
  def testChunky1() : Unit = {
    val result = Index.createChunky(1, 11, 11, 3)
    assertEquals(0, result.x)
    assertEquals(0, result.y)
    assertEquals(1, result.band)
  }

  @Test
  def testChunky2() : Unit = {
    val result = Index.createChunky(5, 11, 11, 3)
    assertEquals(2, result.band)
    assertEquals(1, result.x)
    assertEquals(0, result.y)

  }

  @Test
  def testChunky3() : Unit = {
    val result = Index.createChunky(15, 11, 11, 3)
    assertEquals(5, result.x)
    assertEquals(0, result.y)
    assertEquals(0, result.band)
  }

  @Test
  def testChunky4() : Unit = {
    val result = Index.createChunky(10, 11, 11, 3)
    assertEquals(3, result.x)
    assertEquals(0, result.y)
    assertEquals(1, result.band)
  }

  @Test
  def testOrderingByBands() : Unit = {
    val input = List(Index(4, 9, 0), Index(3, 9, 1), Index(1, 9, 0), Index(0, 9, 1))
    val expected = List(Index(1, 9, 0), Index(4, 9, 0), Index(0, 9, 1), Index(3, 9, 1))
    val result = input.sorted(Index.orderingByBandOutput)

    assertEquals(expected, result)
  }

  @Test
  def testOrderingByPosition() : Unit = {
    val input = List(Index(4, 9, 0), Index(3, 9, 1), Index(1, 9, 0), Index(0, 9, 1))
    val expected = List(Index(0, 9, 1), Index(1, 9, 0), Index(3, 9, 1), Index(4, 9, 0))
    val result = input.sorted(Index.orderingByPositionThenBand)

    assertEquals(expected, result)
  }
}
