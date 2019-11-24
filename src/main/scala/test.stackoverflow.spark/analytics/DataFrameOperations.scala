package test.stackoverflow.spark.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import test.stackoverflow.spark.utils.GetAllProperties

trait DataFrameOperations {

  def queryDF(sqlContext: SQLContext, queryString: String)

  def createTablesAndWriteToFile(pathArray: Option[Array[Path]], spark: SparkSession)

  def writeDFToFile(df: DataFrame, viewName: String) = {
    println("Writing to a file...")

    //get the current logged in user.
    val userName = System.getProperty("user.name")

    val outputPath = GetAllProperties.readPropertyFile get "CSV_OUTPUT" getOrElse ("#") replace("<USER_NAME>", userName)

    df.write
      .mode("overwrite")
      .option("header", "true")
      .parquet("file:///" + outputPath + viewName)

  }

}
