package org.catapult.sa.spark

import java.io.{DataOutputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

/**
  * Text output with out the newlines.
  *
  * If you need newlines insert them your self.
  */
class RawTextOutputFormat extends TextOutputFormat[NullWritable, Text]{

  protected object LineRecordWriter {
    private val utf8: String = "UTF-8"
  }

  protected class LineRecordWriter(var out: DataOutputStream) extends RecordWriter[NullWritable, Text] {

    @throws[IOException]
    private def writeObject(o: Any) {
      o match {
        case to: Text =>
          out.write(to.getBytes, 0, to.getLength)
        case _ =>
          out.write(o.toString.getBytes(LineRecordWriter.utf8))
      }
    }

    @throws[IOException]
    def write(key: NullWritable, value: Text) {
      val nullValue: Boolean = value == null
      if (nullValue) {
        return
      }

      if (!nullValue) {
        writeObject(value)
      }
    }

    @throws[IOException]
    def close(context: TaskAttemptContext) {
      out.close()
    }
  }

  @throws[IOException]
  @throws[InterruptedException]
  override def getRecordWriter(job: TaskAttemptContext): RecordWriter[NullWritable, Text] = {
    val conf: Configuration = job.getConfiguration

    val file: Path = getDefaultWorkFile(job, "")
    val fs: FileSystem = file.getFileSystem(conf)
    val fileOut: FSDataOutputStream = fs.create(file, false)
    new LineRecordWriter(fileOut)

  }
}
