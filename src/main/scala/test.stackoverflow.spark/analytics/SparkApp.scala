package test.stackoverflow.spark.analytics

import test.stackoverflow.spark.utils.{Contexts, GetAllProperties}

object SparkApp extends App{

  println("hello World!!!!")

  //get the current logged in user.
  val userName = System.getProperty("user.name")

  val inputFile = GetAllProperties.readPropertyFile get "ANS_JSON_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)

  val spark = Contexts.SPARK

  val inputDataType = args(0)

  inputDataType match {

    case "json" =>

      // Read the Json file.
      val fullDF = spark.read.json(inputFile)

      import org.apache.spark.sql.functions._

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


      xmlDF.createOrReplaceGlobalTempView("XML_DATA")

      xmlDF.sqlContext.sql("select * from XML_DATA where UserId='5240'").printSchema()

      //xmlDF.printSchema()
  }

  // TO let SPARK UI in active state.
  Thread.sleep(86400000)

  Contexts.stopContext

}
