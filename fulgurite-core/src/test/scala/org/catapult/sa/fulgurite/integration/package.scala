package org.catapult.sa.fulgurite

import org.apache.spark.{SparkConf, SparkContext}
import org.catapult.sa.fulgurite.geotiff.Index

/**
  * Utility methods to make tests easier.
  *
  * This is similar to the example one but means we can safely split the config for testing and we don't have to include
  * things in the examples to be able to run the tests.
  */
package object integration {
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
