package org.catapult.sa.spark

import java.io._

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by Wil.Selwood on 08/06/2016.
  */
object SparkUtils {
  // Version of append that can be used to aggregate
  def append[T](l : mutable.Buffer[T], r : T) : mutable.Buffer[T] = {
    l.append(r)
    l
  }

  // Version of append for second half of aggregate
  def appendAll[T](a : mutable.Buffer[T], b : mutable.Buffer[T]) : mutable.Buffer[T] = {
    a.appendAll(b)
    a
  }

  def saveMultiLineTextFile(rdd : RDD[String], fileName : String) : Unit = {
    rdd.map(e => NullWritable.get() -> new Text(e))
      .saveAsNewAPIHadoopFile(fileName, classOf[NullWritable], classOf[Text], classOf[MultiLineTextOutputFormat])
  }

  def joinOutputFiles(headerPath : String, path : String, prefix : String, outputName : String) : Unit = {
    val dir = new File(path)
    if (! dir.isDirectory) {
      throw new IOException("Path is not a directory")
    }

    joinFiles(outputName+".tmp", dir.listFiles().filter(f => f.getName.startsWith(prefix)).sortBy(_.getName).toList:_*)
    joinFiles(outputName, List(new File(headerPath), new File(outputName + ".tmp")):_*)
  }

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

  def deleteAllExcept(path : String, toKeep : String*) : Unit = {
    val dir = new File(path)
    if (! dir.isDirectory) {
      throw new IOException("Path is not a directory")
    }

    dir.listFiles()
      .filter(f => !toKeep.contains(f.getName))
      .foreach(f => f.delete())
  }


}
