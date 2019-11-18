package test.stackoverflow.spark.analytics

import test.stackoverflow.spark.utils.{Contexts, GetAllProperties, ListFilesUnderDir}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SparkApp extends App{

  println("Hello World!!!!")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  val userName = System.getProperty("user.name")

  val winutilpath = GetAllProperties.readPropertyFile get "WINUTILPATH" getOrElse("#") replace("<USER_NAME>",userName)

  System.setProperty("hadoop.home.dir", winutilpath)

  //get the current logged in user.

  val inputFile = GetAllProperties.readPropertyFile get "ANS_JSON_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)

  val spark = Contexts.SPARK

  val inputDataType = args(0)

  inputDataType match {

    case "json" =>

      // list all the files under directory.

      val filesPath = new ListFilesUnderDir(spark).filesUnderDir

      //Option[Array[Path]]
      filesPath.get.foreach{
        path =>

          val dataFrames = spark.read.option("multiLine", true).json(path.toString)

          dataFrames.printSchema()

          val tblNm = path.getName.replace(".","_")

          val viewName = path.getName.replace(".","_")
          dataFrames.createOrReplaceTempView(viewName)

          //@TODO: create all the raw tables(raw data model) and insert data into these hive tables.
/*
          val sqlContext = Contexts.SQL_CONTEXT
          val queryString = s"CREATE TABLE IF NOT EXISTS ${viewName} row format delimited fields terminated by '|' AS select * from ${viewName} LIMIT 10"
          sqlContext.sql("CREATE TABLE contributordeletionreasons(Description STRING,Id STRING,Name STRING) row format delimited fields terminated by '|'").show()
          sqlContext.sql("insert overwrite table contributordeletionreasons select * from contributordeletionreasons_json")
*/
      }

      // Read the Json file.
      val fullDF = spark.read.json(inputFile)

      val items = fullDF.select(explode(col("items")))

      items.printSchema()

      //Task 1: find the answers by no. of vote with question_id and answer_owner

      val selectOrderedDF = items.select(col("col.answer_id") as("answer"),col("col.score") as("answer_score"),col("col.question_id") as("question"),col("col.owner.user_id") as("owner"))
        .orderBy(desc("answer_score"))
      selectOrderedDF.printSchema()

      selectOrderedDF.show()

    case "xml" =>
    // Read the xml file.
      val inputFile = GetAllProperties.readPropertyFile get "XML_INPUT_DATA" getOrElse("#") replace("<USER_NAME>",userName)

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
