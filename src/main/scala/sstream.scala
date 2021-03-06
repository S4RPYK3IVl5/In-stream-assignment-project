import java.sql.Timestamp

import com.datastax.spark.connector.CassandraSparkExtensions
import com.redis.RedisClient
import model.CaseClasses.Events
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types._
import utils._

object sstream {

  val localhost = "127.0.0.1"
  val redis = new RedisClient(localhost, 6379)

  def main(args: Array[String]): Unit = {

    val (sparkSession: SparkSession, dfStream: DataFrame) = createSparkEnvironment

    val valueDs = convertDataToEventsDS(dfStream, sparkSession: SparkSession)

    val windowedDf = windowData(valueDs, sparkSession)

    writeToRedis(windowedDf, sparkSession)

    writeToCassandra(valueDs, sparkSession)

    sparkSession.streams.awaitAnyTermination()

  }

  def closeStreaming(sparkSession: SparkSession): Unit = {
    sparkSession.streams.active.foreach(_.stop())
  }

  def writeToCassandra(valueDs: Dataset[Events], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    valueDs.writeStream
      .outputMode(OutputMode.Update)
      .trigger(ProcessingTime("5 seconds"))
      .foreachBatch((ds, _) => {

        val cachedBots = sparkSession.read
          .format("org.apache.spark.sql.redis")
          .schema(
            StructType(Array(
              StructField("ip", StringType),
              StructField("event_sum", IntegerType),
              StructField("indicator", DoubleType),
              StructField("count_of_window", IntegerType),
              StructField("added_time", LongType))
            )
          )
          .option("keys.pattern", "bots:*")
          .option("key.column", "ip")
          .load().toDF("rIp", "rEvent_sum", "rIndicator", "rCount_of_window", "rAdded_time")

        ds.join(cachedBots, ds("ip") === cachedBots("rIp"), "left")
          .withColumn("is_bot", $"rIp".isNotNull)
          .select($"ip", $"category_id", $"unix_time", $"type", $"is_bot")
          .write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "event_click")
          .option("table", "events")
          .mode("APPEND")
          .save()

      }).start
  }

  def writeToRedis(windowedDf: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    windowedDf.writeStream
      .outputMode(OutputMode.Update)
      .trigger(ProcessingTime("5 seconds"))
      .foreachBatch((ds, _) => {

        val cachedBots = sparkSession.read
          .format("org.apache.spark.sql.redis")
          .schema(
            StructType(Array(
              StructField("ip", StringType),
              StructField("event_sum", IntegerType),
              StructField("indicator", DoubleType),
              StructField("count_of_window", IntegerType),
              StructField("added_time", LongType))
            )
          )
          .option("keys.pattern", "bots:*")
          .option("key.column", "ip")
          .load().toDF("rIp", "rEvent_sum", "rIndicator", "rCount_of_window", "rAdded_time")

        val botsDf = ds.filter($"count" >= 20)
          .withColumn("added_time", current_timestamp().cast(LongType))
          .dropDuplicates(Seq("ip"))

        val joinedDf = cachedBots.join(botsDf, $"rIp" === $"ip", "left")

        val whiteListedDf = joinedDf
          .filter($"ip".isNull && (current_timestamp().cast(LongType) - $"rAdded_time") > 600)
        if (!whiteListedDf.isEmpty)
          whiteListedDf.foreach(row => redis.del(s"bots:${row.getAs[String]("rIp")}"))

        botsDf.write
          .format("org.apache.spark.sql.redis")
          .option("table", "bots")
          .option("key.column", "ip")
          .mode(SaveMode.Append)
          .save()

      }).start
  }

  def windowData(valueDs: Dataset[Events], sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val actionPerIp = valueDs.withWatermark("unix_time", "20 seconds")
      .groupBy(window($"unix_time", "10 seconds", "10 seconds"), $"ip")
      .count()
    actionPerIp
  }

  def convertDataToEventsDS(dfStream: DataFrame, sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val valueDs = dfStream.selectExpr("CAST(value AS STRING) as value")
      .map {
        case Row(value) =>
          value match {
            case str: String => parseString(str)
            case _ => Nil
          }
      }.as[List[String]].filter(_.nonEmpty).map(a => Events(new Timestamp(a(0).toLong * 1000), a(1).toInt, a(2), a(3)))
    //      .filter(_.ip == "172.20.0.0")
    valueDs
  }

  def createSparkEnvironment = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("bot-detection")
      .config("spark.redis.host", localhost)
      .config("spark.redis.port", "6379")
      .config("spark.cassandra.connection.host", localhost)
      .withExtensions(new CassandraSparkExtensions)
      .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.consistency.level", "ONE")
      .getOrCreate()

    val dfStream: DataFrame = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", localhost + ":9092")
      .option("subscribe", "user-click-data")
      .load()
    (sparkSession, dfStream)
  }
}
