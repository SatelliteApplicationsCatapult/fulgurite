package org.catapult.sa.fulgurite

import org.apache.spark.{SparkConf, SparkContext}
import org.catapult.sa.fulgurite.geotiff.Index
import org.catapult.sa.fulgurite.spark.Argument

/**
  * helpers to take some of the boiler plate out of the examples
  */
package object examples {

  // Default arguments used in the examples
  val InputArgument = Argument("input", "c:\\git\\LC08_L1TP_203024_20190513_20190521_01_T1_uncompressed_py_clipped.TIF")
  val OutputArgument = Argument("output", "c:\\git\\output32.tif")//FileUtils.getTempDirectoryPath + "/test_" + new Date().getTime.toString + ".tif")

  val InputOutputArguments = List(InputArgument, OutputArgument)

  /**
    * Create a default spark config. Making sure that compression is turned on and kryo is set up with the Index class
    * @param appName Name of this application
    * @param master name of the master
    * @return result config
    */
  def createConfig(appName : String, master : String = "local[*]") = new SparkConf()
    .setAppName(appName)
    .setMaster(master)
    .set("spark.rdd.compress", "true")
    .set("spark.io.compression.codec", "lz4")
    .set("spark.io.compression.lz4.blockSize", "16K")
    .registerKryoClasses(Array(classOf[Index]))

  def getSparkContext(appName : String, master : String = "local[*]") =
    SparkContext.getOrCreate(createConfig(appName, master))

}
