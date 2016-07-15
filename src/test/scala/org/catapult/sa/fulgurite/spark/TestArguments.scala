package org.catapult.sa.fulgurite.spark

import org.junit.Test
import org.junit.Assert._

class TestArguments {

  class ArgTest extends Arguments {
    override def defaultArgs(): Map[String, String] = Map("argWibble" -> "foo", "testArg" -> "fish")
    override def allArgs(): List[Argument] = List("testArg", "argWibble")
  }

  @Test
  def testBasicUsage() : Unit = {

    val uut : ArgTest = new ArgTest()
    val result = uut.processArgs(Array("--argWibble", "bob", "-testArg", "testResult1"))

    assertEquals(2, result.size)
    assertEquals("bob", result("argWibble"))
    assertEquals("testResult1", result("testArg"))

  }

  @Test
  def testDefaultFallBack() : Unit = {

    val uut : ArgTest = new ArgTest()
    val result = uut.processArgs(Array.empty[String])

    assertEquals(2, result.size)
    assertEquals("foo", result("argWibble"))
    assertEquals("fish", result("testArg"))
  }

  @Test
  def testFlags() : Unit = {
    class FlagTest extends Arguments {
      override def defaultArgs(): Map[String, String] = Map("fish" -> "ardvark")
      override def allArgs(): List[Argument] = List(Argument("fish", flag = true))
    }

    val uut = new FlagTest()
    val result = uut.processArgs(Array("--fish"))

    assertEquals("true", result("fish"))
    assertEquals(1, result.size)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testUnknownArgs() : Unit = {
    val uut : ArgTest = new ArgTest()
    uut.processArgs(Array("--argWibble", "bob", "-testArg", "testResult1", "this", "should", "not", "show", "up"))
  }


}
