package test.stackoverflow.spark.utils

object ReadHiveTables extends App{

    val sqlContext = Contexts.SQL_CONTEXT

    sqlContext.sql("select * from contributortypes_json_temp1 LIMIT 10").show()

}
