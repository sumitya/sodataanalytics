package test.stackoverflow.spark.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SQLContext, SparkSession}

object DocumentDataFrame extends DataFrameOperations {

  override def queryDF(sqlContext: SQLContext, queryString: String): Unit = {

    executeQuery(sqlContext, queryString)

  }

  private def executeQuery(sqlContext: SQLContext, queryString: String) = {

    println("=======================================================================")

    sqlContext.sql(queryString).show(10000)

    println("=======================================================================")

  }

  override def createTablesAndWriteToFile(pathArray: Option[Array[Path]], spark: SparkSession): Unit = {

    pathArray.get.foreach {
      path =>

        val dataFrame = spark.read.option("multiLine", true).json(path.toString)

        //Calling the size calculator for RDD that was read.
        new DataViewer(spark.sqlContext).displayDataSize(path, dataFrame.rdd.map(_.toString()))

        val tblNm = path.getName.replace(".", "_")

        val viewName = path.getName.replace(".", "_")

        writeDFToFile(dataFrame, viewName: String)

        dataFrame.createOrReplaceTempView(viewName)

        val queryString = s"CREATE TABLE IF NOT EXISTS ${viewName}_temp1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile AS select * from ${viewName}"

        spark.sqlContext.sql(queryString)

    }
  }


}
