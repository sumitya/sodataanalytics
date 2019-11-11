package test.stackoverflow.spark.utils

import org.apache.spark.sql.{SQLContext, SparkSession}

object Contexts {

  // SPARK(or SPARKSESSION) is created as soon as it is declared here.
  val SPARK:SparkSession = SparkSession
    .builder
    .appName("SODataAnalytics")
    .master("local")
    .getOrCreate()

  // SQL_CONTEXT is created as soon as it is declared here.
  val SQL_CONTEXT:SQLContext = SPARK.sqlContext

  // Advantage of declare it def is, it will be executed only when called..lazy evaluation
  def stopContext:Unit = SPARK.stop()


}

