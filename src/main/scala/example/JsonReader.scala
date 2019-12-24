package example

import example.JsonReader.args
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

object JsonReader extends App {

  case class User(
                   id:      Option[Int],
                   country: Option[String],
                   points:  Option[Int],
                   price:   Option[Int],
                   title:   Option[String],
                   variety: Option[String],
                   winery:  Option[String]
                 )
  implicit val jsonDefaultFormats: DefaultFormats.type = DefaultFormats

  val spark = SparkSession.builder().appName("ConvertJsonToScalaCaseClass").master("local").getOrCreate()
  val sc = spark.sparkContext
  val json = sc.textFile(args(0))

  json.foreach(element => println(JsonMethods.parse(element).extract[User]))

}
