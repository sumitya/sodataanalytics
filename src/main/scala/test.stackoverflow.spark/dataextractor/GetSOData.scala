package test.stackoverflow.spark.dataextractor

import java.io.{BufferedWriter,FileWriter}

import net.liftweb.json._
import dispatch._
import Defaults._
import test.stackoverflow.spark.utils.GetAllProperties

object GetSOData {

  def main(args: Array[String]): Unit = {
    //step 1: set url

    val answersRequests = url(GetAllProperties.readPropertyFile get "ANSWER_URL" getOrElse("#"))

    val postsRequests = url(GetAllProperties.readPropertyFile get "POST_URL" getOrElse("#"))

    val requestAsGet = answersRequests.GET //not required but lets be explicit

    //Step 2 : Set the required parameters
    val buildpostsRequest = requestAsGet.addQueryParameter("site", "stackoverflow")

    //Step 3: Make the request (method is already set above)

    val content = Http(buildpostsRequest)

    content onSuccess {

      //Step 4 : Request was successful & response was OK
      case x if x.getStatusCode() == 200 =>

        //Step 5 : Response was OK, read the contents
        val responseBody = x.getResponseBody

        // @TODO: parse the json string, check has_more field to iterate over pages via liftweb.json parsing lib.
        val jsonData = parse(responseBody)
        val hasMoreField = jsonData.json.\("has_more")
        println(hasMoreField.values)

        writeToFile(x.getResponseBody)
        System.exit(1)

      case y => //Step 6 : Response is not OK, read the error
        println("Failed with status code" + y.getStatusCode())
        System.exit(1)
    }

    content onFailure {
      case x =>
        println("Failed but"); println(x.getMessage)
        System.exit(1)
    }

    def writeToFile(responseBody:String) = {

      //get the current logged in user.
      val userName = System.getProperty("user.name")

      val outputFile = GetAllProperties.readPropertyFile get "ANS_JSON_OUTPUT_PATH" getOrElse("#") replace("<USER_NAME>",userName)
      val bw = new BufferedWriter(new FileWriter(outputFile))
      bw.write(responseBody)
      bw.close()
    }
  }
}
