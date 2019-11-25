package test.stackoverflow.spark.utils

import java.io.File

import org.apache.spark.sql.{SQLContext, SparkSession}

object Contexts {

  // SPARK(or SPARKSESSION) is created as soon as it is declared here.
  val SPARK:SparkSession = SparkSession
    .builder
    .appName("SODataAnalytics")
    .master("local[3]")
    // This property is used for setting warehouse directory for hive tables in local mode.
    .config("spark.sql.warehouse",new File("spark-warehouse").getAbsolutePath)
    .enableHiveSupport()
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "6g")
    .config("spark.memory.offHeap.enabled",true)
    .config("spark.memory.offHeap.size","1g")
    .config("spark.hadoop.dfs.block.size","128m")
    .config("spark.hadoop.mapreduce.input.fileinputformat.split.minsize","128000000")
    .config("spark.hadoop.mapreduce.input.fileinputformat.split.maxsize","128000000")
    .getOrCreate()

  // SQL_CONTEXT is created as soon as it is declared here.
  val SQL_CONTEXT:SQLContext = SPARK.sqlContext

  // Advantage of declare it def is, it will be executed only when called..lazy evaluation
  def stopContext:Unit = SPARK.stop()




}

