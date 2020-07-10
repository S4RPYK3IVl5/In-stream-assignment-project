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
      .option("startingOffsets", "earliest")
      .load()

    val valueDs = dfStream.selectExpr("CAST(value AS STRING) as value")
      .map{
        case Row(value) =>
          value match {
            case str: String => parseString(str)
            case _ => Nil
          }
      }.as[List[String]].filter(_.nonEmpty).map(a => Events(a(0).toLong, a(1).toInt, a(2), a(3)))

    val intermediateDf = valueDs.writeStream
      .outputMode(OutputMode.Update())
      .option("truncate", false)
      .trigger(ProcessingTime("5 seconds"))
      .format("console").start()
    sparkSession.streams.awaitAnyTermination()

  }
}