package org.catapult.sa.spark

import java.io.File
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

import com.github.jaiimageio.impl.plugins.tiff.TIFFImageMetadata
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.catapult.sa.geotiff.{GeoTiffMeta, Index}

/**
  * Utility functions for working with geo tiff files.
  */
object GeoSparkUtils {

  /**
    * Read a geotiff and return pixels as records.
    *
    * @param path path to the file to read
    * @param meta the metadata from the file.
    * @param sc spark context to use.
    * @return RDD of Index to DataPoint
    */
  def GeoTiffRDD(path : String, meta: GeoTiffMeta, sc : SparkContext) : RDD[(Index, Int)] = {
    val bytesPerRecord = (meta.bitsPerSample.max + 7) / 8 // TODO: this won't work if any of the bands have different pixel widths.

    val inputConf = new Configuration()
    inputConf.setInt(GeoTiffBinaryInputFormat.RECORD_LENGTH_PROPERTY, bytesPerRecord)
    inputConf.setLong(GeoTiffBinaryInputFormat.RECORD_START_OFFSET_PROPERTY, meta.startOffset)
    inputConf.setLong(GeoTiffBinaryInputFormat.RECORD_END_OFFSET_PROPERTY, meta.endOffset)

    sc.newAPIHadoopFile[LongWritable, BytesWritable, GeoTiffBinaryInputFormat](
      path,
      classOf[GeoTiffBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      inputConf
    ).map(createKeyValue(meta))
  }

  def saveGeoTiff(rdd : RDD[(Index,  Int)], meta : GeoTiffMeta, baseMeta: IIOMetadata, path : String) : Unit = {
    rdd.map { case (i, d) => i.i -> createBytes(meta)(i.band, d) }
      .sortByKey() // TODO: optimise based on the size of the image.
      .map { case (i, b) => new BytesWritable(Array.emptyByteArray) -> new BytesWritable(b) }
      .saveAsNewAPIHadoopFile(path, classOf[BytesWritable], classOf[BytesWritable], classOf[RawBinaryOutputFormat])

    saveMetaData(meta, baseMeta, path)
  }

  def saveMetaData(meta : GeoTiffMeta, baseMeta: IIOMetadata, path : String) : Unit = {
    val clonedMeta = baseMeta

    val ios = ImageIO.createImageOutputStream(new File(path, "header.tiff"))
    val writers = ImageIO.getImageWritersByFormatName("tiff")
    if (writers.hasNext) {
      val writer = writers.next()
      writer.setOutput(ios)
      writer.prepareWriteSequence(clonedMeta)

      var headerNext = ios.getStreamPosition
      val header = headerNext - 4
      ios.seek(header)

      // Ensure IFD is written on a word boundary
      headerNext = (headerNext + 3) & ~0x3

      // Write the pointer to the first IFD after the header.
      ios.writeInt(headerNext.toInt)
      val rootIFD = clonedMeta.asInstanceOf[TIFFImageMetadata].getRootIFD
      rootIFD.writeToStream(ios)

      ios.writeInt(0)

      ios.flush()
      val startPoint = rootIFD.getLastPosition

      ios.seek(rootIFD.getStripOrTileOffsetsPosition)
      0L.until(meta.height*3).foreach { i =>
        val chunk = startPoint + (i * meta.width)
        ios.writeInt(chunk.asInstanceOf[Int])
      }

      ios.seek(rootIFD.getStripOrTileByteCountsPosition)
      0L.until(meta.height*3).foreach { i =>
        ios.writeInt(meta.width.asInstanceOf[Int])
      }
      ios.flush()
      if (ios.getStreamPosition < meta.startOffset) {
        println("writing padding in header. (" + meta.startOffset + " - " + ios.getStreamPosition + ") = " + (meta.startOffset - ios.getStreamPosition))
        //ios.write(Array.fill[Byte]((meta.startOffset - ios.getStreamPosition).toInt) { 0.asInstanceOf[Byte] } )
      }

      ios.close()
    }
  }

  private def createKeyValue(meta : GeoTiffMeta) : ((LongWritable, BytesWritable)) => (Index, Int) = {
    val width = meta.width
    val bandLength = meta.height * meta.width
    val bands = meta.samplesPerPixel
    val pixelWidths = meta.bitsPerSample

    (e : (LongWritable, BytesWritable)) => {
      val i = Index.create(e._1.get(), width, bandLength, bands)
      val b = e._2.getBytes
      pixelWidths(i.band) match {
        case 8 => i -> (0xFF & b(0))
        case 16 => i -> (((b(1) & 0xFF) << 8) | (b(0) & 0xFF))
        case 32 => i -> (((b(3) & 0xFF) << 24) | ((b(2) & 0xFF) << 16) | ((b(1) & 0xFF) << 8) | (b(0) & 0xFF))
        case _ => throw new UnsupportedOperationException("can not yet handle this many bit colours. Find this error and implement")
      }

    }
  }

  private def createBytes(meta : GeoTiffMeta) : (Int, Int) => Array[Byte] = {
    val bytesPerPixel = meta.bitsPerSample
    (band, d) => bytesPerPixel(band) match {
      case 8 => Array(d.asInstanceOf[Byte])
      case 16 => Array((d & 0xFF00 >> 8 ).asInstanceOf[Byte], d.asInstanceOf[Byte])
      case 32 => Array((d & 0xFF000000 >> 24 ).asInstanceOf[Byte], (d & 0xFF0000 >> 16 ).asInstanceOf[Byte], (d & 0xFF00 >> 8 ).asInstanceOf[Byte], d.asInstanceOf[Byte])
      case _ => throw new UnsupportedOperationException("can not yet handle this many bit colours. Find this error and implement")
    }
  }

}
