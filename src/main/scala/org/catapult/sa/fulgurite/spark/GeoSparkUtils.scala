package org.catapult.sa.fulgurite.spark

import java.io.File
import javax.imageio.ImageIO
import javax.imageio.metadata.IIOMetadata

import com.github.jaiimageio.impl.plugins.tiff.TIFFImageMetadata
import com.github.jaiimageio.plugins.tiff.{BaselineTIFFTagSet, GeoTIFFTagSet, TIFFField, TIFFTag}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, Index}

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
    val bytesPerRecord = meta.bytesPerSample.max  // TODO: this won't work if any of the bands have different byte widths.

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

  /**
    * Save a geotiff based on the data in the rdd and the provided metadata.
    *
    * The baseMeta should come from the origianl file.
    *
    * TODO: Remove the need for the baseMeta
    *
    * @param rdd data for the bands
    * @param meta metadata for the
    * @param baseMeta original metadata to base the new metadata on.
    * @param path where to create the output.
    */
  def saveGeoTiff(rdd : RDD[(Index,  Int)], meta : GeoTiffMeta, baseMeta: IIOMetadata, path : String) : Unit = {
    implicit val indexOrdering = Index.orderingByBandOutput

    // TODO: First guess. Check and fix this.
    val numPartitions = (Math.log(meta.height * meta.width * meta.samplesPerPixel) * 100).toInt

    val convertToBytes = createBytes(meta)
    rdd.sortByKey(ascending = true, numPartitions)
      .map { case (i, d) => new BytesWritable(Array.emptyByteArray) -> new BytesWritable(convertToBytes(i.band, d)) }
      .saveAsNewAPIHadoopFile(path, classOf[BytesWritable], classOf[BytesWritable], classOf[RawBinaryOutputFormat])

    saveGeoTiffMetaData(meta, baseMeta, path)
  }

  def saveGeoTiffMetaData(meta : GeoTiffMeta, baseMeta: IIOMetadata, path : String) : Unit = {

    val ios = ImageIO.createImageOutputStream(new File(path, "header.tiff"))
    val writers = ImageIO.getImageWritersByFormatName("tiff")
    if (writers.hasNext) {
      val writer = writers.next()
      writer.setOutput(ios)
      writer.prepareWriteSequence(baseMeta)

      var headerNext = ios.getStreamPosition
      val header = headerNext - 4
      ios.seek(header)

      // Ensure IFD is written on a word boundary
      headerNext = (headerNext + 3) & ~0x3

      // Write the pointer to the first IFD after the header.
      ios.writeInt(headerNext.toInt)

      val rootIFD = baseMeta.asInstanceOf[TIFFImageMetadata].getRootIFD

      // update the ifd and make sure it matches the meta object.
      val base = BaselineTIFFTagSet.getInstance
      val geoTiffBase = GeoTIFFTagSet.getInstance
      val numRows = meta.height.asInstanceOf[Int] * meta.samplesPerPixel
      //val numColumns = meta.width * meta.bytesPerSample.sum
      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_IMAGE_WIDTH), meta.width.toInt))
      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_IMAGE_LENGTH), meta.height.toInt))

      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_SAMPLES_PER_PIXEL), meta.samplesPerPixel))
      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_BITS_PER_SAMPLE), TIFFTag.TIFF_SHORT, meta.samplesPerPixel, meta.bitsPerSample.map(_.toChar)))
      rootIFD.addTIFFField(new TIFFField(geoTiffBase.getTag(GeoTIFFTagSet.TAG_MODEL_PIXEL_SCALE), TIFFTag.TIFF_DOUBLE,  meta.pixelScales.length, meta.pixelScales))

      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_PLANAR_CONFIGURATION), 2)) // force one band after another for our own sanity

      // given we have just updated the size we should also update the number of offset and byte count places to be filled in later
      rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_STRIP_OFFSETS)
      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_STRIP_OFFSETS), TIFFTag.TIFF_LONG, numRows))

      rootIFD.removeTIFFField(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS)
      rootIFD.addTIFFField(new TIFFField(base.getTag(BaselineTIFFTagSet.TAG_STRIP_BYTE_COUNTS), TIFFTag.TIFF_LONG, numRows))

      // Write the IFD
      rootIFD.writeToStream(ios)

      ios.writeInt(0)

      ios.flush()

      val startPoint = rootIFD.getLastPosition
      // Now go back and update the IFD with the position offsets of the strips
      ios.seek(rootIFD.getStripOrTileOffsetsPosition)
      0.until(meta.samplesPerPixel).foreach { s =>
        val rowWidth = meta.width * meta.bytesPerSample(s)
        val bandsOffset = startPoint + meta.bytesPerSample.take(s).map(_ * meta.width * meta.height).sum
        0L.until(meta.height).foreach { i =>
          val chunk = bandsOffset + (i * rowWidth)
          ios.writeInt(chunk.asInstanceOf[Int])
        }
      }

      // Finally update the row byte lengths
      ios.seek(rootIFD.getStripOrTileByteCountsPosition)
      0.until(meta.samplesPerPixel).foreach { s =>
        val rowWidth = meta.width * meta.bytesPerSample(s)
        0L.until(meta.height).foreach { i =>
          ios.writeInt(rowWidth.asInstanceOf[Int])
        }
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
