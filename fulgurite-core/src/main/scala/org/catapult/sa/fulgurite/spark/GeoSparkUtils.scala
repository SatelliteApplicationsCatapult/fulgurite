package org.catapult.sa.fulgurite.spark

import java.io._

import com.github.jaiimageio.plugins.tiff.BaselineTIFFTagSet
import javax.imageio.ImageIO
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.catapult.sa.fulgurite.geotiff.{GeoTiffMeta, GeoTiffMetaHelper, Index}

/**
  * Functions for working with GeoTIFF files.
  */
object GeoSparkUtils {

  /**
    * Read a GeoTIFF and return pixels as records.
    *
    * @param path path to the file to read
    * @param meta the metadata from the file.
    * @param sc spark context to use.
    * @param partitionSize size of the partitions to create. Tune this for your environment.
    * @return RDD of Index to DataPoint
    */
  def GeoTiffRDD(path : String, meta: GeoTiffMeta, sc : SparkContext, partitionSize : Long = 400000) : RDD[(Index, Int)] = {

    if (meta.isCompressed) {
      throw new IllegalArgumentException("Can not handle compressed GeoTIFF files.")
    }

    val bytesPerRecord = meta.bytesPerSample.min  // TODO: this won't work if any of the bands have different byte widths.
                                                  // We don't have any examples where this is the case. If this causes
                                                  // problems please raise a bug on github and include an example image

    val inputConf = new Configuration()
    inputConf.setInt(GeoTiffBinaryInputFormat.RECORD_LENGTH_PROPERTY, bytesPerRecord)
    inputConf.setLong(GeoTiffBinaryInputFormat.RECORD_START_OFFSET_PROPERTY, meta.startOffset)
    inputConf.setLong(GeoTiffBinaryInputFormat.RECORD_END_OFFSET_PROPERTY, meta.endOffset)
    inputConf.setLong(FileInputFormat.SPLIT_MAXSIZE, partitionSize)

    sc.newAPIHadoopFile[LongWritable, BytesWritable, GeoTiffBinaryInputFormat](
      path,
      classOf[GeoTiffBinaryInputFormat],
      classOf[LongWritable],
      classOf[BytesWritable],
      inputConf
    ).map(createKeyValue(meta))
  }

  /**
    * Save a GeoTIFF based on the data in the rdd and the provided metadata.
    *
    * The numPartitions is used by the sorting that is required by this for the output to end up non mangled. You will
    * want to tune this depending on the images you are loading and the size of your cluster. We recommend some
    * experiments here to work out the best value for your system.
    *
    * NOTE: On windows this will require the HADOOP_HOME environment variable to be set to where the winutils.exe is.
    *
    * @param rdd data for the bands
    * @param meta metadata for the
    * @param path where to create the output.
    * @param numPartitions how many partitions to use when sorting and also how many output files get created.
    */
  def saveGeoTiff(rdd : RDD[(Index,  Int)], meta : GeoTiffMeta, path : String, numPartitions : Int = 1000) : Unit = {
    saveGeoTiffData(rdd, meta, path, numPartitions)
    saveGeoTiffMetaData(meta, path)
  }

  /**
    * Save the rdd as GeoTIFF data formatted according to the provided metadata.
    *
    * This does not save the metadata. Use saveGeoTiff or saveGeoTiffMetaData to do that.
    *
    * The numPartitions is used by the sorting that is required by this for the output to end up non mangled. You will
    * want to tune this depending on the images you are loading and the size of your cluster. We recommend some
    * experiments here to work out the best value for your system.
    *
    * NOTE: On windows this will require the HADOOP_HOME environment variable to be set to where the winutils.exe is.
    *
    * @param rdd data for the bands
    * @param meta metadata for the
    * @param path where to create the output.
    * @param numPartitions how many partitions to use when sorting and also how many output files get created.
    */
  def saveGeoTiffData(rdd : RDD[(Index,  Int)], meta : GeoTiffMeta, path : String, numPartitions : Int = 1000) : Unit = {

    if (meta.isCompressed) {
      throw new IllegalArgumentException("Can not handle compressed GeoTIFF files.")
    }

    implicit val indexOrdering = meta.planarConfiguration match {
      case 1 => Index.orderingByPositionThenBand
      case 2 => Index.orderingByBandOutput
      case _ => throw new IllegalArgumentException("unknown planar configuration " + meta.planarConfiguration)
    }

    val convertToBytes = createBytes(meta)
    rdd.sortByKey(ascending = true, numPartitions)
      .map { case (i, d) => new BytesWritable(Array.emptyByteArray) -> new BytesWritable(convertToBytes(i.band, d)) }
      .saveAsNewAPIHadoopFile(path, classOf[BytesWritable], classOf[BytesWritable], classOf[RawBinaryOutputFormat])

  }

  /**
    * Write the metadata for a GeoTIFF image to a file. This will only write the metadata but not any of the data.
    *
    * The path is to a directory. A file will be created inside called header.tiff  This will need to be joined up
    * at the front of any data to create a complete file.
    *
    * NOTE: the same and unmodified metadata object must be used to write both the data and the header.
    *
    * @param meta the metadata to write out
    * @param path where to write the metadata.
    */
  def saveGeoTiffMetaData(meta : GeoTiffMeta, path : String) : Unit = {

    val ios = ImageIO.createImageOutputStream(new File(path, "header.tiff"))
    val writers = ImageIO.getImageWritersByFormatName("tiff")
    if (writers.hasNext) {

      val baseMeta = GeoTiffMetaHelper.createImageMetaData(meta)
      val rootIFD = baseMeta.getRootIFD

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

      // Write the IFD
      rootIFD.writeToStream(ios)

      ios.writeInt(0)

      ios.flush()
      val startPoint = rootIFD.getLastPosition
      ios.seek(rootIFD.getStripOrTileOffsetsPosition)
      
      // Now go back and update the IFD with the position offsets of the strips
      if (meta.planarConfiguration == BaselineTIFFTagSet.PLANAR_CONFIGURATION_PLANAR) {

        0.until(meta.samplesPerPixel).foreach { s =>
          val rowWidth = meta.width * meta.bytesPerSample(s)
          val bandsOffset = startPoint + meta.bytesPerSample.take(s).map(_ * meta.width * meta.height).sum
          0L.until(meta.height).foreach { i =>
            val chunk = bandsOffset + (i * rowWidth)
            ios.writeInt(chunk.asInstanceOf[Int])
          }
        }
      } else {

        val bandsOffset = startPoint + meta.bytesPerSample.sum * meta.rowsPerStrip * meta.height
        0L.until(meta.height).foreach { _ =>
          ios.writeInt(bandsOffset.asInstanceOf[Int])
        }
      }

      // Finally update the row byte lengths
      ios.seek(rootIFD.getStripOrTileByteCountsPosition)
      0.until(meta.samplesPerPixel).foreach { s =>
        val rowWidth = meta.width * meta.bytesPerSample(s)
        0L.until(meta.height).foreach { _ =>
          ios.writeInt(rowWidth.asInstanceOf[Int])
        }
      }

      ios.close()
    }
  }

  /**
    * Save a RDD of strings as a text file with out any formatting. No new lines or any thing.
    * If you need new lines the input strings must have newlines in.
    *
    * The output of this will be chunked by the number of partitions in the input RDD
    *
    * NOTE: On windows this will require the HADOOP_HOME environment variable to be set to where the winutils.exe is.
    *
    * @param rdd rdd of strings to write to the file.
    * @param path where to write the files
    */
  def saveRawTextFile(rdd : RDD[String], path : String) : Unit = {
    rdd.map(e => NullWritable.get() -> new Text(e))
      .saveAsNewAPIHadoopFile(path, classOf[NullWritable], classOf[Text], classOf[RawTextOutputFormat])
  }

  /**
    * Join a set of files together with a header.
    *
    * This is useful to join the parts created by saveGeoTiff and saveRawTextFile together along with the headers.
    *
    * @param headerPath path to the header file (This will always be written first)
    * @param path path to the location of the rest of the files
    * @param outputName full path of the resulting output file
    * @param prefix prefix used to filter the input path.
    */
  def joinOutputFiles(headerPath: String, path: String, outputName: String, prefix: String = "part-"): Unit = {
    val dir = new File(path)
    if (! dir.isDirectory) {
      throw new IOException("Path is not a directory")
    }

    joinFiles(outputName+".tmp", dir.listFiles().filter(f => f.getName.startsWith(prefix)).sortBy(_.getName).toList:_*)
    joinFiles(outputName, List(new File(headerPath), new File(outputName + ".tmp")):_*)
  }

  /**
    * Join a set of files together.
    *
    * Somewhat like cat
    *
    * @param outputPath where to write the result
    * @param files ordered set of files to join together.
    */
  def joinFiles(outputPath : String, files : File*) : Unit = {
    val output = new File(outputPath)
    if (output.exists()) {
      throw new IOException("Output path already exists")
    }

    val outputStream = new FileOutputStream(output)
    val buffer = new Array[Byte](2048)
    files.foreach(f => {
      val inputStream = new BufferedInputStream(new FileInputStream(f))

      var count = 0
      do {
        count = inputStream.read(buffer, 0, 2048)
        if (count >= 0) {
          outputStream.write(buffer, 0, count)
        }
      } while (count >= 0)

      inputStream.close()
    })

    outputStream.flush()
    outputStream.close()
  }

  /**
    * Group up all the bands for a pixel into a single record so you can access them all at once.
    *
    * The positions in the resulting arrays will correspond to the original bands.
    *
    * e.g:
    *
    * Index(1, 2, 0) -> 123
    * Index(1, 2, 1) -> 56
    * Index(1, 2, 2) -> 23
    *
    * Becomes
    *
    * (1, 2) -> Array(123, 56, 23)
    *
    * This is a wide and expensive operation.
    *
    * @param rdd the rdd to group up
    * @param bands number of bands in the stream. (This needs to be accurate)
    * @return grouped up rdd.
    */
  def pixelGroup(rdd : RDD[(Index, Int)], bands : Int) : RDD[((Long, Long), Array[Option[Int]])] = {

    def setValues(a : Array[Option[Int]], b : (Int, Int)) : Array[Option[Int]] = {
      a.update(b._1, Some(b._2))
      a
    }

    def mergeValues(a : Array[Option[Int]], b : Array[Option[Int]]) : Array[Option[Int]] = {
      b.zipWithIndex.filter(_._1.isDefined).foreach(f => a.update(f._2, f._1))
      a
    }

    // Use optionals as there may be missing entries for a band. Also when merging we need to make sure we don't over
    // write things.
    rdd.map { case (i, d) =>
      (i.x -> i.y) -> (i.band -> d)
    }.aggregateByKey((0 until bands).map(i => Option.empty[Int]).toArray)(setValues, mergeValues)

  }

  /**
    * Un-group a grouped set of pixes and generate the index objects.
    *
    * e.g:
    *
    * (1, 2) -> Array(123, 56, 23)
    *
    * becomes:
    *
    * Index(1, 2, 0) -> 123
    * Index(1, 2, 1) -> 56
    * Index(1, 2, 2) -> 23
    *
    * @param rdd grouped RDD Pair X and Y cords to array of band values
    * @return un grouped index based RDD
    */
  def pixelUnGroup(rdd : RDD[((Long, Long), Array[Option[Int]])]) : RDD[(Index, Int)] = {
    rdd.flatMap { case ((x, y), d) =>
      d.zipWithIndex.filter(_._1.isDefined).map { case (data, i) => Index(x, y, i) -> data.get }
    }
  }

  /**
    * Create a function that will create the Index for a given set of metadata when reading.
    *
    * Creating the function in this way means that we have less branches when doing the reading.
    *
    * @param meta the metadata for the file being read.
    * @return function for converting the result of a hadoop binary read into index and values.
    */
  private def createKeyValue(meta : GeoTiffMeta) : ((LongWritable, BytesWritable)) => (Index, Int) = {
    val width = meta.width
    val height = meta.height
    val bandLength = meta.height * meta.width
    val bands = meta.samplesPerPixel
    val pixelWidths = meta.bitsPerSample

    val indexBuilder = meta.planarConfiguration match {
      case 1 => (i: Long) => Index.createChunky(i, width, height, bands)
      case 2 => (i: Long) => Index.createPlanar(i, width, bandLength, bands)
      case _ => throw new IllegalArgumentException("Unknown planar configuration " + meta.planarConfiguration)
    }

    (e : (LongWritable, BytesWritable)) => {
      val i = indexBuilder(e._1.get())
      val b = e._2.getBytes
      pixelWidths(i.band) match {
        case 8 => i -> (0xFF & b(0))
        case 16 => i -> (((b(1) & 0xFF) << 8) | (b(0) & 0xFF))
        case 32 => i -> (((b(3) & 0xFF) << 24) | ((b(2) & 0xFF) << 16) | ((b(1) & 0xFF) << 8) | (b(0) & 0xFF))
        case _ => throw new UnsupportedOperationException("can not yet handle this many bit colours. Find this error and implement")
      }
    }
  }

  /**
    * Create a function for converting bands and value into bytes to output.
    *
    * @param meta the metadata that describes the output file. Must match up to the incoming data.
    * @return function to convert bands and data values into byte arrays.
    */
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
