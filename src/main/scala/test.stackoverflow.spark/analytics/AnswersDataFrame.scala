package test.stackoverflow.spark.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

object AnswersDataFrame extends DataFrameOperations {

  override def queryDF(sqlContext: SQLContext, queryString: String): Unit = {

    //Task 1: find the answers by no. of vote with question_id and answer_owner

    sqlContext.sql(queryString).show(10000)

  }

  override def createTablesAndWriteToFile(pathArray: Option[Array[Path]], spark: SparkSession): Unit = {
    pathArray.get.foreach {
      path =>
        val dataFrame = spark.read.option("multiLine", true).json(path.toString)

        val items = dataFrame.select(explode(col("items")))

        items.printSchema()

        val tblNm = path.getName.replace(".", "_")

        val viewName = path.getName.replace(".", "_")

        writeDFToFile(items, viewName: String)

        items.createOrReplaceTempView(viewName)

        val queryString = s"CREATE TABLE IF NOT EXISTS ${viewName}_temp1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile AS select * from ${viewName}"

        spark.sqlContext.sql(queryString)

    }
  }

}
