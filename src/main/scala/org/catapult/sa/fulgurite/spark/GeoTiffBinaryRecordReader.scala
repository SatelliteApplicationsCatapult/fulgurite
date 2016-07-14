package org.catapult.sa.fulgurite.spark

import java.io.IOException

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * read a geotif data set.
  */
class GeoTiffBinaryRecordReader extends RecordReader[LongWritable, BytesWritable] {

  private var startOffset : Long = 0L
  private var splitStart: Long = 0L
  private var splitEnd: Long = 0L
  private var currentPosition: Long = 0L
  private var recordLength: Int = 0
  private var fileInputStream: FSDataInputStream = null
  private var recordKey: LongWritable = null
  private var recordValue: BytesWritable = null

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(
        ((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0
      ).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {
    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]
    startOffset = GeoTiffBinaryInputFormat.getStartOffset(context)
    // the byte position this fileSplit starts at but offset with the start offset for the data in the file
    splitStart = fileSplit.getStart + startOffset

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = splitStart + fileSplit.getLength

    // if the split is after the end of the data section of the file truncate it back to the end.
    if (splitEnd > GeoTiffBinaryInputFormat.getEndOffset(context)) {
      splitEnd = GeoTiffBinaryInputFormat.getEndOffset(context)
    }

    // the actual file we will be reading from
    val file = fileSplit.getPath
    // job configuration
    val job = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    // check compression
    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (codec != null) {
      throw new IOException("GeoTiffBinaryRecordReader does not support reading compressed files")
    }
    // get the record length
    recordLength = GeoTiffBinaryInputFormat.getRecordLength(context)
    // get the filesystem
    val fs = file.getFileSystem(job)
    // open the File
    fileInputStream = fs.open(file)
    // seek to the splitStart position
    fileInputStream.seek(splitStart)
    // set our current position
    currentPosition = splitStart
  }

  override def nextKeyValue(): Boolean = {
    if (recordKey == null) {
      recordKey = new LongWritable()
    }
    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set((currentPosition - startOffset) / recordLength)
    // the recordValue to place the bytes into
    if (recordValue == null) {
      recordValue = new BytesWritable(new Array[Byte](recordLength))
    }
    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {
      // setup a buffer to store the record
      val buffer = recordValue.getBytes
      fileInputStream.readFully(buffer)
      // update our current position
      currentPosition = currentPosition + recordLength
      // return true
      return true
    }
    false
  }

}
