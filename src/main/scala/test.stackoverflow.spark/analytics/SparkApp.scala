package test.stackoverflow.spark.analytics

import test.stackoverflow.spark.utils.{Contexts, GetAllProperties}

object SparkApp extends App{

  println("hello World!!!!")

  //get the current logged in user.
  val userName = System.getProperty("user.name")

  val inputFile = GetAllProperties.readPropertyFile get "ANS_JSON_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)

  val spark = Contexts.SPARK

  val fullDF = spark.read.json(inputFile)

  import org.apache.spark.sql.functions._

  val items = fullDF.select(explode(col("items")))

  items.printSchema()

  //Task 1: find the answers by no. of vote with question_id and answer_owner

  val selectOrderedDF = items.select(col("col.answer_id") as("answer"),col("col.score") as("answer_score"),col("col.question_id") as("question"),col("col.owner.user_id") as("owner"))
      .orderBy(desc("answer_score"))
  selectOrderedDF.printSchema()

  selectOrderedDF.show()


}
