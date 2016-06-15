package org.catapult.sa.spark

import org.apache.spark.SparkConf

/**
  * Help methods to make using spark and geo mesa easier.
  */
trait SparkApplication extends Arguments {

  /**
    * Define any extra arguments your program needs
    *
    * @return list of arguments
    */
  def extraArgs() : List[Argument]

  /**
    * Define default values for your extra arguments or overwrite the built in ones
    *
    * @return map of default values.
    */
  def defaultExtraArgs() : Map[String, String]

  override def defaultArgs(): Map[String, String] = Map(
    "appName" -> "Behaviour Analytics",
    "master" -> "yarn-cluster"
  ) ++ defaultExtraArgs()

  override def allArgs(): List[Argument] = List(
    Argument("appName"),
    Argument("master")
  ) ++ extraArgs()

  /**
    * Options map populated in configure
    */
  var opts : Map[String, String] = _

  /**
    * Create a spark config using the provided arguments
    *
    * @param args args to read and populate opts
    * @return SparkConf
    */
  def configure(args : Array[String]) : SparkConf = {
    opts = processArgs(args, defaultArgs())

    val conf = new SparkConf()
      .setAppName(opts("appName"))
      .setMaster(opts("master"))
      .set("spark.memory.fraction", "0.66")
      .set("spark.rdd.compress", "true")
      .set("spark.io.compression.codec", "lz4")
      .set("spark.io.compression.lz4.blockSize", "16K")

    conf
  }


}
