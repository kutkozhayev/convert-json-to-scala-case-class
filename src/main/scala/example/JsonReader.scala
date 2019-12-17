package example

import example.JsonReader.args
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

object JsonReader extends App {

  case class User(
                   id: Int,
                   country: String,
                   points: Int,
                   //price: Int,
                   title: String,
                   variety: String,
                   winery: String
                 )
  implicit val jsonDefaultFormats = DefaultFormats;

  val spark = SparkSession.builder().appName("ConvertJsonToScalaCaseClass").master("local").getOrCreate();
  val sc = spark.sparkContext;
  val json = sc.textFile(args(0));


  for(element<-json.collect().toList)
  {
    println(JsonMethods.parse(element).extract[User]);
  }
}
