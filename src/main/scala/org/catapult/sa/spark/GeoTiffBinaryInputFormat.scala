package org.catapult.sa.spark

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.input.FixedLengthBinaryRecordReader


object GeoTiffBinaryInputFormat {

  val RECORD_LENGTH_PROPERTY = "org.catapult.sa.spark.GeoTiffBinaryInputFormat.recordLength"
  val RECORD_START_OFFSET_PROPERTY = "org.catapult.sa.spark.GeoTiffBinaryInputFormat.startOffset"
  val RECORD_END_OFFSET_PROPERTY = "org.catapult.sa.spark.GeoTiffBinaryInputFormat.endOffset"

  def getRecordLength(context: JobContext): Int = {
    SparkHadoopUtil.get.getConfigurationFromJobContext(context).get(RECORD_LENGTH_PROPERTY).toInt
  }

  def getStartOffset(context: JobContext) : Long = {
    SparkHadoopUtil.get.getConfigurationFromJobContext(context).get(RECORD_START_OFFSET_PROPERTY).toLong
  }

  def getEndOffset(context: JobContext) : Long = {
    SparkHadoopUtil.get.getConfigurationFromJobContext(context).get(RECORD_END_OFFSET_PROPERTY).toLong
  }
}

class GeoTiffBinaryInputFormat extends FileInputFormat[LongWritable, BytesWritable]with Logging {

  private var recordLength = -1

  /**
    * Override of isSplitable to ensure initial computation of the record length
    */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {
    if (recordLength == -1) {
      recordLength = GeoTiffBinaryInputFormat.getRecordLength(context)
    }
    if (recordLength <= 0) {
      logDebug("record length is less than 0, file cannot be split")
      false
    } else {
      true
    }
  }

  /**
    * This input format overrides computeSplitSize() to make sure that each split
    * only contains full records. Each InputSplit passed to FixedLengthBinaryRecordReader
    * will start at the first byte of a record, and the last byte will the last byte of a record.
    */
  override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {
    val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)
    // If the default size is less than the length of a record, make it equal to it
    // Otherwise, make sure the split size is as close to possible as the default size,
    // but still contains a complete set of records, with the first record
    // starting at the first byte in the split and the last record ending with the last byte
    if (defaultSize < recordLength) {
      recordLength.toLong
    } else {
      (Math.floor(defaultSize / recordLength) * recordLength).toLong
    }
  }

  /**
    * Create a FixedLengthBinaryRecordReader
    */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[LongWritable, BytesWritable] = {
    new GeoTiffBinaryRecordReader
  }

}
