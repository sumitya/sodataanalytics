package test.stackoverflow.spark.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.storage.StorageLevel

object UsersDataFrame extends DataFrameOperations {

  override def queryDF(sqlContext: SQLContext, queryString: String): Unit = {

    // createView is a onetime activity

    //createView(sqlContext,queryString)

    executeQuery(sqlContext,queryString)

  }

  def createView(sqlContext: SQLContext, queryString: String)= {

    sqlContext.sql(queryString).cache()

  }

  private def executeQuery(sqlContext: SQLContext, queryString: String) = {

    println("=======================================================================")

    println("+++"+queryString)
    sqlContext.sql(queryString).show(1000, false)

    println("=======================================================================")

  }

  override def createTablesAndWriteToFile(pathArray: Option[Array[Path]], spark: SparkSession): Unit = {
    pathArray.get.foreach {
      path =>

        println(path)

        // @TODO: pass rowTag at runTime for different xmls.
        val xmlDF = spark.read
          .format("com.databricks.spark.xml")
          .option("rowTag", "users")
          .load(path.toString)

        xmlDF.persist(StorageLevel.MEMORY_AND_DISK_SER)

        xmlDF.printSchema()

        xmlDF.show()

        val items = xmlDF.select(explode(col("row"))).persist(StorageLevel.MEMORY_AND_DISK_SER)

        items.printSchema()

        //Calling the size calculator for RDD that was read.
        new DataViewer(spark.sqlContext).displayDataSize(path, xmlDF.rdd.map(_.toString()))

        val tblNm = path.getName.replace(".", "_")

        val viewName = path.getName.replace(".", "_")

        writeDFToFile(items, viewName: String)

        items.createOrReplaceTempView(viewName)

        val queryString = s"CREATE TABLE IF NOT EXISTS ${viewName}_temp1 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TextFile AS select * from ${viewName}"

        spark.sqlContext.sql(queryString)

    }
  }
}
