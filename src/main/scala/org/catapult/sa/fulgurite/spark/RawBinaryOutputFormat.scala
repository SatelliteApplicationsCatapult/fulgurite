package org.catapult.sa.fulgurite.spark

import java.io.DataOutputStream

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
  * Output format that writes raw bytes to files. No headers, or any thing fancy
  */
class RawBinaryOutputFormat extends FileOutputFormat[BytesWritable, BytesWritable] {

  class BinaryRecordWriter(out : DataOutputStream) extends RecordWriter[BytesWritable, BytesWritable] {
    override def write(key: BytesWritable, value: BytesWritable): Unit = {
      out.write(value.getBytes)
    }

    override def close(context: TaskAttemptContext): Unit = {
      out.close()
    }
  }

  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[BytesWritable, BytesWritable] = {
    val file = getDefaultWorkFile(job, "")
    val fs = file.getFileSystem(job.getConfiguration)
    val fileOut = fs.create(file, false)

    new BinaryRecordWriter(fileOut)
  }
}
