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
    val result = Index.create(1, 11478, 11478 * 10782, 3)
    assertEquals(1, result.x)
    assertEquals(0, result.y)
    assertEquals(0, result.band)
  }

  @Test
  def testCreateSecondBand() : Unit = {
    val result = Index.create((11478 * 10782) + 1, 11478, 11478 * 10782, 3)
    assertEquals(1, result.x)
    assertEquals(0, result.y)
    assertEquals(1, result.band)
  }
}
