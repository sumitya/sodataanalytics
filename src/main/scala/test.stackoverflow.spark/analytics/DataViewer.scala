package test.stackoverflow.spark.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import org.apache.hadoop.fs.Path

object DataViewer {

  def displayRawDocData(sqlContext:SQLContext) = {

    println("contributordeletionreasons_json_temp1 ")
    sqlContext.sql(s"select * from default.contributordeletionreasons_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("contributors_json_temp1 ")
    sqlContext.sql("select * from default.contributors_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("contributortypes_json_temp1 ")
    sqlContext.sql("select * from default.contributortypes_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("doctags_json_temp1 ")
    sqlContext.sql("select * from default.doctags_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("doctagversions_json_temp1 ")
    sqlContext.sql("select * from default.doctagversions_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("examples_json_temp1 ")
    sqlContext.sql("select * from default.examples_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("topichistories_json_temp1 ")
    sqlContext.sql("select * from default.topichistories_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("topichistorytypes_json_temp1 ")
    sqlContext.sql("select * from default.topichistorytypes_json_temp1 LIMIT 50").show()
    println("=======================================================================")
    println("topics_json_temp1 ")
    sqlContext.sql("select * from default.topics_json_temp1 LIMIT 50").show()
    println("=======================================================================")

  }


  def displayDataSize(path:Path,rdd: RDD[String]) {

    println("PATH::"+path.toString,"RDD SIZE::"+humanReadableByteCount(
      rdd.map(_.getBytes("UTF-8").length.toLong).reduce(_ + _)
      )
    )
  }

  private def humanReadableByteCount(bytes: Long): String = {
    val unit = 1024
    if (bytes < unit) return bytes + " B"
    val exp = (Math.log(bytes) / Math.log(unit)).toInt
    val pre = "KMGTPE".charAt(exp - 1) + "B"
    val divisor = Math.pow(unit,exp)
    f"${bytes/ divisor}%.1f ${pre}"

  }

}
