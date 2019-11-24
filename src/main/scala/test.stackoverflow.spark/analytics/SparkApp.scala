package test.stackoverflow.spark.analytics

import test.stackoverflow.spark.utils.{Contexts, GetAllProperties, ListFilesUnderDir}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object SparkApp extends App {

  println("Hello World!!!!")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  //get the current logged in user.

  private val userName = System.getProperty("user.name")

  //This is used for local development only.
  val winutilpath = GetAllProperties.readPropertyFile get "WINUTILPATH" getOrElse ("#") replace("<USER_NAME>", userName)
  System.setProperty("hadoop.home.dir", winutilpath)

  private val hqlFilesPath = GetAllProperties.readPropertyFile get "HQL_FILES" getOrElse ("#") replace("<USER_NAME>", userName)

  private val spark = Contexts.SPARK

  private val sqlContext = Contexts.SQL_CONTEXT

  private var dataViewer: DataViewer = _

  /* This App have 3 params::
   1. Format of data read i.e. json/xml.
   2. hqlfileName.
   3. category of data answers/documentation.
   */

  private val inputDataType = args(0)

  private val hqlFileName = args(1)

  private val dataCategory = args(2)

  inputDataType match {

    case "json" =>

      // list all the files under directory.

      //Return:Option[Array[Path]]
      val filesPath = new ListFilesUnderDir(spark,dataCategory).filesUnderDir

      val queryString = new StringBuilder

      for (line <- Source.fromFile(hqlFilesPath + hqlFileName.toString).getLines) {

        queryString append (line)

      }

      // Document
      //Create Tables and write to files, one time activity.
      //DocumentDataFrame.createTablesAndWriteToFile(filesPath,spark)

      //Query the table
      DocumentDataFrame.queryDF(sqlContext, queryString.toString)

      // Answers
      //AnswersDataFrame.createTablesAndWriteToFile(filesPath, spark)
      //AnswersDataFrame.queryDF(sqlContext, queryString.toString)

    case "xml" =>
      // Read the xml file.
      val inputFile = GetAllProperties.readPropertyFile get "XML_INPUT_DATA" getOrElse ("#") replace("<USER_NAME>", userName)

      val xmlDF = spark.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "badges")
        .load(inputFile)

      xmlDF.printSchema()

      xmlDF.show()

      val allRowItems = xmlDF.select(explode(col("row")))

      allRowItems.printSchema()

      allRowItems.show()

      xmlDF.registerTempTable("XML_DATA")

      val sqlcontext = Contexts.SQL_CONTEXT

  }

  // TO let SPARK UI in active state.
  Thread.sleep(86400000)

  Contexts.stopContext

}
