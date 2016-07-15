package org.catapult.sa.fulgurite.spark

import org.junit.Test
import org.junit.Assert._

import scala.collection.mutable


class TestSparkUtils {

  @Test
  def append() : Unit = {
    val buffer = SparkUtils.append(mutable.Buffer.empty[String], "hello")

    assertEquals(1, buffer.size)
    assertEquals("hello", buffer.head)

    SparkUtils.append(buffer, "world")

    assertEquals(2, buffer.size)
    assertEquals("hello", buffer.head)
    assertEquals("world", buffer.last)
  }

  @Test
  def appendAll() : Unit = {
    val buffer = SparkUtils.appendAll(mutable.Buffer.empty[String], mutable.Buffer("hello", "world"))

    assertEquals(2, buffer.size)
    assertEquals("hello", buffer.head)
    assertEquals("world", buffer.last)

    SparkUtils.appendAll(buffer, mutable.Buffer("this", "is", "more", "text"))

    assertEquals(6, buffer.size)
    assertEquals(mutable.Buffer("hello", "world", "this", "is", "more", "text"), buffer)
  }

  @Test
  def average() : Unit = {
    assertEquals(5 -> 1, SparkUtils.average(0 -> 0, 5))
    assertEquals(20 -> 4, SparkUtils.average(15 -> 3, 5))
    assertEquals(1024 -> 106, SparkUtils.average(1019 -> 105, 5))
  }

  @Test
  def averageSum() : Unit = {
    assertEquals(10 -> 2, SparkUtils.averageSum(5 -> 1, 5 -> 1))
    assertEquals(0 -> 0, SparkUtils.averageSum(0 -> 0, 0 -> 0))
    assertEquals(5 -> 1, SparkUtils.averageSum(0 -> 0, 5 -> 1))
    assertEquals(5 -> 1, SparkUtils.averageSum(5 -> 1, 0 -> 0))
  }

  @Test
  def finalAverage() : Unit = {
    assertEquals("key" -> 5, SparkUtils.finalAverage("key" -> (10 -> 2)))
    assertEquals("key" -> 15, SparkUtils.finalAverage("key" -> (30 -> 2)))
    assertEquals("key" -> 0, SparkUtils.finalAverage("key" -> (10 -> 30)))
  }

}
