package test.stackoverflow.spark.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.fs.Path

//made the param private for preventing getters and setters from being generated.
class DataViewer(private val sqlContext:SQLContext) {

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
