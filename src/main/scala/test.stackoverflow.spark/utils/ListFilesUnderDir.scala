package test.stackoverflow.spark.utils

import org.apache.hadoop.fs.{Hdfs, LocalFileSystem, Path}
import org.apache.spark.sql.SparkSession

class ListFilesUnderDir(spark: SparkSession,datFormatCategory:String,dataCategory:String) {

  var pathString: String = _
  var allFiles: Option[Array[Path]] = _

  def filesUnderDir: Option[Array[Path]] = {

    val userName = System.getProperty("user.name")

    val inputDir = GetAllProperties.readPropertyFile get datFormatCategory.toUpperCase+"_OUTPUT_PATH" getOrElse ("#") replace("<USER_NAME>", userName)

    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs match {
      //There is no need for a string interpolation when the variable is a string already.
      case fs if fs.isInstanceOf[LocalFileSystem] => pathString = "file:///" + inputDir + dataCategory.toLowerCase

      case fs if fs.isInstanceOf[Hdfs] => pathString = "hdfs://" + inputDir + dataCategory.toLowerCase

      //@TODO: implement cloud FileSystems also, For Ex: case fs if fs.isInstanceOf[S3FileSystem] => ...
      case _ =>
        println("No Suitable File System found!!!")
        System.exit(1)
    }

    val someval = fs.listStatus(new Path(pathString)).filter(_.isFile)

    val mappedFiles = someval.map(_.getPath)

    Option(mappedFiles)
  }

}
