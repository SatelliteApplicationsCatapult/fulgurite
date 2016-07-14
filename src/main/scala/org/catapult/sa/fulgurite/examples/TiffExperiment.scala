package org.catapult.sa.fulgurite.examples

import java.io._
import javax.imageio.ImageIO

import org.catapult.sa.fulgurite.geotiff.GeoTiffMeta
import org.catapult.sa.fulgurite.spark.{Argument, Arguments}
import org.catapult.sa.spark.Argument
import org.w3c.dom.Node

/**
  * Experiments working with GeoTiff files
  */
object TiffExperiment extends Arguments {

  def main(args : Array[String]) : Unit = {
    val opts = processArgs(args, defaultArgs())
    val file = new File(opts("in"))

    if (file == null || !file.canRead || !file.isFile) {
      throw new IOException("Can not read " + opts("in"))
    }

    val iis = ImageIO.createImageInputStream(file)
    val readers = ImageIO.getImageReaders(iis)


    if (readers.hasNext) {
      val reader = readers.next()

      reader.setInput(iis, true, false)

      val meta = reader.getImageMetadata(0)

      val geoMeta = GeoTiffMeta(meta)
      println(geoMeta)

      /*var byteCount : Long = 0
      var done : Boolean = false
      val buffer : Array[Byte] = new Array[Byte](1024)
      val bytesToRead = geoMeta.endOffset - geoMeta.startOffset

      val fis = new FileInputStream(file)
      fis.skip(geoMeta.startOffset)

      while (!done) {
        val read = fis.read(buffer)
        if (read == -1) {
          done = true
        } else {
          byteCount = byteCount + read
          if (byteCount >= bytesToRead) {
            done = true
          }
        }
      }

      println("read : " + byteCount)*/

      val output = new PrintStream(new FileOutputStream("output5.xml"))

      meta.getMetadataFormatNames.foreach(k => {
        displayMeta(output, meta.getAsTree(k), 0)
      })

      output.flush()

      /*val ios = ImageIO.createImageOutputStream(new File("output.meta.tiff"))
      val writers = ImageIO.getImageWritersByFormatName("tiff")
      if (writers.hasNext) {
        val writer = writers.next()
        writer.setOutput(ios)
        writer.prepareWriteSequence(meta)

        meta.asInstanceOf[TIFFImageMetadata].getRootIFD.writeToStream(ios)

        ios.flush()
        ios.close()
      }*/

    }

  }

  override def defaultArgs(): Map[String, String] = Map(
    "in" -> "C:/data/Will/test_1466091017199.tif/data.tif"
  )

  override def allArgs(): List[Argument] = List("in")


  private def displayMeta(ps : PrintStream, node : Node, level : Int) : Unit = {
    indent(ps, level)

    ps.print("<" + node.getNodeName)
    val attribute = node.getAttributes
    if (attribute != null) {
      (0 until attribute.getLength).foreach(i => {
        val attr = attribute.item(i)
        ps.print(" " + attr.getNodeName + "=\"" + attr.getNodeValue + "\"")
      })
    }

    if (node.hasChildNodes) {
      ps.println(">")
      var child = node.getFirstChild
      while (child != null) {
        displayMeta(ps, child, level + 1)
        child = child.getNextSibling
      }

      indent(ps, level)
      ps.println("</" + node.getNodeName + ">")
    } else {
      ps.println("/>")
    }
  }

  private def indent(ps : PrintStream, level : Int): Unit = {
    ps.print((0 until level).map(l => "\t").mkString)
  }
}
