import java.sql.Timestamp

import model.CaseClasses.Events
import utils._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}

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
      .load()

    val valueDs = dfStream.selectExpr("CAST(value AS STRING) as value")
      .map{
        case Row(value) =>
          value match {
            case str: String => parseString(str)
            case _ => Nil
          }
      }.as[List[String]].filter(_.nonEmpty).map(a => Events(new Timestamp(a(0).toLong * 1000), a(1).toInt, a(2), a(3)))
      .filter(_.ip == "172.20.0.0")

    val intermediateDf = valueDs.withWatermark("unix_time", "20 seconds")
      .groupBy(window($"unix_time", "10 seconds", "10 seconds"), $"ip")
      .count()
      .writeStream
      .outputMode(OutputMode.Append())
      .option("truncate", false)
      .trigger(ProcessingTime("5 seconds"))
      .format("console").start()
    sparkSession.streams.awaitAnyTermination()

  }
}