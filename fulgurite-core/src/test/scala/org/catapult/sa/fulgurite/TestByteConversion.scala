package org.catapult.sa.fulgurite

import org.junit.Test
import org.junit.Assert._

class TestByteConversion {

  @Test
  def simple(): Unit = {
    assertEquals(-1.asInstanceOf[Byte], 255.asInstanceOf[Byte])
    assertEquals(0.asInstanceOf[Byte], 256.asInstanceOf[Byte])

    assertEquals(255, 0xFF & -1.asInstanceOf[Byte])
  }


}
