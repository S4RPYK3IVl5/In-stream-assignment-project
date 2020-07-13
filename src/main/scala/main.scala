import java.sql.Timestamp

import model.CaseClasses.Events
import utils._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.StructType

object main {

  val localhost = "127.0.0.1"
  val pathToCSTFilesIntermediateCheckpoint = "/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/checkpoint-location/intermediate"
  val pathToCSTFilesIntermediate = "/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/spark-output/intermediate"
  val pathToCSTFilesGroupedCheckpoint = "/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/checkpoint-location/grouped"
  val pathToCSTFilesGrouped = "/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/spark-output/grouped"


  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("bot-detection")
      .config("spark.redis.host", localhost)
      .config("spark.redis.port", "6379")
      .getOrCreate()

    import sparkSession.implicits._

    val dfStream: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", localhost + ":9092")
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
//      .filter(_.ip == "172.20.0.0")

    val actionPerIp = valueDs.withWatermark("unix_time", "20 seconds")
      .groupBy(window($"unix_time", "10 seconds", "10 seconds"), $"ip")
      .count()

    actionPerIp.select(
      "window.start",
      "window.end",
      "ip",
      "count"
    ).writeStream
      .outputMode(OutputMode.Append)
      .trigger(ProcessingTime("5 second"))
      .format("csv")
      .option("path", pathToCSTFilesIntermediate)
      .option("checkpointLocation", pathToCSTFilesIntermediateCheckpoint)
      .start

    val eventCSVSchema = new StructType()
      .add("wFrom", "timestamp")
      .add("wTo", "timestamp")
      .add("ip", "string")
      .add("count", "integer")
    val readScvFiles = sparkSession
        .readStream
        .option("sep", ",")
        .schema(eventCSVSchema)
        .csv(pathToCSTFilesIntermediate)

    val ipWithIndicator = readScvFiles.groupBy($"ip")
      .agg(sum($"count").as("event_sum"), count($"ip").as("count_of_window"))
      .withColumn("indicator", $"event_sum" / $"count_of_window")

    ipWithIndicator.writeStream
      .outputMode(OutputMode.Update)
      .trigger(ProcessingTime("5 seconds"))
      .foreachBatch((ds, _) =>
        ds.write
          .format("org.apache.spark.sql.redis")
          .option("table", "bootsCache")
          .option("key.column", "ip")
          .mode(SaveMode.Append)
          .save()
      )
      .start
    sparkSession.streams.awaitAnyTermination()

  }
}