package org.catapult.sa.spark

import java.io._

import org.apache.accumulo.core.client.{BatchWriterConfig, ClientConfiguration, IteratorSetting}
import org.apache.accumulo.core.client.mapreduce.{AccumuloInputFormat, AccumuloOutputFormat}
import org.apache.accumulo.core.client.mapreduce.lib.impl.{ConfiguratorBase, InputConfigurator, OutputConfigurator}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.data.{Key, Mutation, Value}
import org.apache.accumulo.core.iterators.user.WholeRowIterator
import org.apache.accumulo.core.util.{Pair => APair}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.{MRJobConfig, OutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams, ModifyAccumuloFeatureWriter}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._
import scala.collection.mutable

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
    "instance" -> "oceanmind",
    "zookeepers" -> "zookeeper01:2181,zookeeper02:2181,zookeeper03:2181",
    "appName" -> "Behaviour Analytics",
    "master" -> "yarn-cluster",
    "user" -> "root",
    "pass" -> "root"
  ) ++ defaultExtraArgs()

  override def allArgs(): List[Argument] = List(
    Argument("instance"),
    Argument("zookeepers"),
    Argument("appName"),
    Argument("master"),
    Argument("user"),
    Argument("pass")
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

  /**
    * Add the geomesa extras to the provided spark conf. All the schemas in the provided datasources will be added
    *
    * Note: the dataSources are a list of argument keys in the opts map to use for the datasource names.
    *
    * @param conf spark conf to update
    * @param dataSources the data source argument names to add
    */
  def setupGeoMesa(conf : SparkConf, dataSources : List[String]) : Unit = {
    val schemas = dataSources
      .map(t => DataStoreFinder.getDataStore(toGeoArgs(opts, t)).asInstanceOf[AccumuloDataStore])
      .flatMap(ds => ds.getTypeNames.map(ds.getSchema))
    if (schemas.nonEmpty) {
      GeoMesaSpark.init(conf, schemas)
    }
  }

  /**
    * Default set up of a geomesa spark context.
    *
    * @param args args from main.
    * @param tables tables to look for datasources in.
    * @return fully configured spark context.
    */
  def createGeoMesaContext(args : Array[String], tables : List[String]) : SparkContext = {
    val conf = configure(args)
    setupGeoMesa(conf, tables)
    new SparkContext(conf)
  }

  /**
    * Scan an accumulo table.
    *
    * This is the easy way that covers most of the cases.
    *
    * @param sc spark context
    * @param table table name
    * @param columns columns to fetch. If empty will return all columns. Defaults to empty
    * @return RDD from table.
    */
  def accumuloInput(sc : SparkContext, table : String, columns : List[(Text, Text)] = List()) : RDD[(Key, Value)] = {
    accumuloInput(sc, createAccumuloQueryConfig(sc, table, columns))
  }

  /**
    * Scan an accumulo table using the provided configuration.
    *
    * This alows you to provide extra configuration options.
    *
    * @param sc spark context
    * @param conf hadoop configuration.
    * @return RDD from table.
    */
  def accumuloInput(sc : SparkContext, conf : Configuration) : RDD[(Key, Value)] = {
    sc.newAPIHadoopRDD(conf, classOf[AccumuloInputFormat], classOf[Key], classOf[Value])
  }

  /**
    * Create the basic Input configuration required to scan an accumulo table.
    *
    * @param sc spark context
    * @param table table name
    * @param columns columns to fetch. If empty will return all columns. Defaults to empty
    * @return Hadoop configuration.
    */
  def createAccumuloQueryConfig(sc : SparkContext, table : String, columns : List[(Text, Text)] = List()) : Configuration = {
    val user = opts("user")
    val token = new PasswordToken(opts("pass"))

    val clientConfig = new ClientConfiguration()
      .withInstance(opts("instance"))
      .withZkHosts(opts("zookeepers"))
    val conf = new Configuration()
    ConfiguratorBase.setConnectorInfo(classOf[AccumuloInputFormat], conf, user, token)
    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloInputFormat], conf, clientConfig)
    InputConfigurator.setInputTableName(classOf[AccumuloInputFormat], conf, table)

    InputConfigurator.addIterator(classOf[AccumuloInputFormat], conf, new IteratorSetting(1, classOf[WholeRowIterator]))

    if (columns.nonEmpty) {
      InputConfigurator.fetchColumns(classOf[AccumuloInputFormat], conf, columns.map(i => new APair(i._1, i._2)))
    }

    conf
  }

  /**
    * Create an accumulo output configuration.
    *
    * @param sc spark context
    * @param defaultTable default table name. Defaults to some thing stupid. If you are not providing table names you should set this.
    * @return Hadoop configuration for an output to accumulo.
    */
  def createAccumuloOutputConf(sc : SparkContext, defaultTable : String = "wibble") : Configuration = {
    val user = opts("user")
    val token = new PasswordToken(opts("pass"))

    val conf = new Configuration()
    val clientConfig = new ClientConfiguration()
      .withInstance(opts("instance"))
      .withZkHosts(opts("zookeepers"))

    ConfiguratorBase.setConnectorInfo(classOf[AccumuloOutputFormat], conf, user, token)
    ConfiguratorBase.setZooKeeperInstance(classOf[AccumuloOutputFormat], conf, clientConfig)
    OutputConfigurator.setCreateTables(classOf[AccumuloOutputFormat], conf, true)
    OutputConfigurator.setDefaultTableName(classOf[AccumuloOutputFormat], conf, defaultTable)
    OutputConfigurator.setBatchWriterOptions(classOf[AccumuloOutputFormat], conf, new BatchWriterConfig())

    conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, classOf[AccumuloOutputFormat], classOf[OutputFormat[Text, Mutation]])

    conf
  }

  /**
    * Query a geo mesa tables with the normal options.
    *
    * @param sc spark context
    * @param table option name that contains the table name
    * @param query the query to run.
    * @return RDD of simple features from geomesa.
    */
  def queryGeoMesa(sc : SparkContext, table : String, query : Query) : RDD[SimpleFeature] = {
    GeoMesaSpark.rdd(new Configuration(), sc, toGeoArgs(opts, table), query, None)
  }

  /**
    * Save an RDD to geo mesa.
    *
    * @param rdd the rdd to save
    * @param tableName the opption containing the datasource table name
    * @param writeTypeName the name of the feature type to write.
    */
  def saveGeoMesa(rdd: RDD[SimpleFeature], tableName : String, writeTypeName: String): Unit = {
    val geoOpts = toGeoArgs(opts, tableName)
    val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
    require(ds.getSchema(writeTypeName) != null, "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .saveGeoMesa")

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      val attrNames = featureWriter.getFeatureType.getAttributeDescriptors.map(_.getLocalName)
      try {
        iter.foreach { case rawFeature : SimpleFeature =>
          val newFeature = featureWriter.next()
          attrNames.foreach(an => newFeature.setAttribute(an, rawFeature.getAttribute(an)))
          newFeature.setDefaultGeometry(rawFeature.getDefaultGeometry)

          if (newFeature.getAttribute(featureWriter.getFeatureType.getGeometryDescriptor.getLocalName) == null) {
            newFeature.setAttribute(featureWriter.getFeatureType.getGeometryDescriptor.getLocalName, rawFeature.getAttribute(featureWriter.getFeatureType.getGeometryDescriptor.getLocalName))
          }
          featureWriter.write()
        }
      } finally {
        featureWriter.close()
      }
    }
  }

  /**
    * Update a record in geomesa.
    *
    * @param rdd the rdd containing data to update
    * @param tableName the argument containing the table name to update
    * @param writeTypeName the type name to update.
    */
  def updateGeoMesa(rdd: RDD[(SimpleFeature, Map[String, Any])], tableName : String, writeTypeName: String): Unit = {
    val geoOpts = toGeoArgs(opts, tableName)
    val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
    require(ds.getSchema(writeTypeName) != null, "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
      val featureWriter : ModifyAccumuloFeatureWriter = ds.getFeatureWriter(writeTypeName, Transaction.AUTO_COMMIT).asInstanceOf[ModifyAccumuloFeatureWriter]
      val featureBuilder = new SimpleFeatureBuilder(ds.getSchema(writeTypeName))
      try {
        iter.foreach { case (newFeature, updates) =>

          featureWriter.original = newFeature

          featureBuilder.reset()
          featureBuilder.init(newFeature)

          updates.foreach(e => featureBuilder.set(e._1, e._2))

          featureWriter.live = featureBuilder.buildFeature(newFeature.getID)

          featureWriter.write()
        }
      } finally {
        featureWriter.close()
      }
    }
  }

  /**
    * Remove simple features from geomesa
    *
    * @param rdd the rdd of simple features
    * @param tableName the option containing the name of the table to remove from.
    * @param writeTypeName the data type to remove.
    */
  def removeGeoMesa(rdd: RDD[SimpleFeature],  tableName : String, writeTypeName: String): Unit = {
    val geoOpts = toGeoArgs(opts, tableName)
    val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
    require(ds.getSchema(writeTypeName) != null, "feature type must exist before calling save.  Call .createSchema on the DataStore before calling .remove")

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(geoOpts).asInstanceOf[AccumuloDataStore]
      val featureWriter : ModifyAccumuloFeatureWriter = ds.getFeatureWriter(writeTypeName, Transaction.AUTO_COMMIT).asInstanceOf[ModifyAccumuloFeatureWriter]
      try {
        iter.foreach { case oldFeature =>
          featureWriter.original = oldFeature
          featureWriter.remove()
        }
      } finally {
        featureWriter.close()
      }
    }
  }

  /**
    * Map the arguments needed for geomesa from our options.
    *
    * @param opts our options.
    * @param table the opts entry which contains the table name,
    * @param root are we root?
    * @return argument map that can be passed to DataStoreFinder.getDataStore()
    */
  def toGeoArgs(opts : Map[String, String], table : String, root : Boolean = false) : Map[String, String] = {
    if (root || opts("user") == "root") {
      Map(
        "instanceId" -> opts("instance"),
        "zookeepers" -> opts("zookeepers"),
        "user" -> "root",
        "password" -> "root",
        "tableName" -> opts(table)
      )
    } else {
      Map(
        "instanceId" -> opts("instance"),
        "zookeepers" -> opts("zookeepers"),
        "user" -> opts("user"),
        "password" -> opts("pass"),
        "tableName" -> opts(table),
        AccumuloDataStoreParams.authsParam.key -> opts("user")
      )
    }
  }

  def buildFeatureType(name : String, ts : String) : SimpleFeatureType = {
    val spec = IOUtils.toString(this.getClass.getResourceAsStream("/" + name + ".schema"))
    val featureType = SimpleFeatureTypes.createType(name, spec)
    featureType.getUserData.put(Constants.SF_PROPERTY_START_TIME, ts)

    featureType
  }

}
