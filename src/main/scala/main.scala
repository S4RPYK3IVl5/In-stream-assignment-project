import model.CaseClasses.Events
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import spray.json._
import DefaultJsonProtocol._
import utils._

object main {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("bot-detection")
      .getOrCreate()

    import sparkSession.implicits._

    val dfStream: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user-click-data")
      .option("startingOffsets", "earliest")
      .load()

    val valueDs = dfStream.selectExpr("CAST(value AS STRING) as value")
//      .map{
//        case Row(value) => println(value);value.asInstanceOf[String].init
//          .replace("[","").replace("]","").parseJson.convertTo[Events]
//      }
      .map{
        case Row(value) =>
          value match {
            case str: String => parseString(str)
            case _ => Nil
          }
      }.as[List[String]]

    val intermediateDf = valueDs.writeStream
      .outputMode(OutputMode.Update())
      .option("truncate", false)
      .trigger(ProcessingTime("5 seconds"))
      .format("console").start()
    sparkSession.streams.awaitAnyTermination()

  }
}