import java.lang
import java.sql.Timestamp

import com.datastax.spark.connector._
import com.redis.RedisClient
import model.CaseClasses.Events
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import utils._

import scala.collection.mutable.Map

object dstream {

  val localhost = "127.0.0.1"
  val redis = new RedisClient(localhost, 6379)

  def main(args: Array[String]): Unit = {

    val (ssc, stream, sparkSession) = crateSparkEnvironment

    computingData(stream, sparkSession)

    ssc.start
    ssc.awaitTermination

  }

  def closeStreaming(ssc: StreamingContext): Unit = {
    ssc.stop(true, true)
  }

  def computingData(stream: InputDStream[ConsumerRecord[String, String]], sparkSession: SparkSession) = {
    def accumulateMappingFunction(key: (Timestamp, Timestamp, String),
                                  value: Option[(Events, Int)],
                                  state: State[Int]): ((Timestamp, Timestamp, String), (Events, Int)) = {
      value match {
        case Some(x) => state.update(state.getOption().getOrElse(0) + x._2); (key, (x._1, state.get))
        case None => (key, (Events(new Timestamp(0), 0, "", ""), 0))
      }
    }

    stream
      .map(_.value)
      .map(x => parseString(x))
      .filter(_.nonEmpty)
      .map(a =>
        (createTriple(a(0).toDouble, a(2)),
          (Events(new Timestamp(a(0).toLong * 1000), a(1).toInt, a(2), a(3)), 1)))
      .mapWithState(StateSpec.function(accumulateMappingFunction _))
      .foreachRDD(rdd => {

        rdd.reduceByKey((x, y) => if (x._2 > y._2) x else y)
          .collect().foreach(value => {
          val ip = value._1._3
          val requests = value._2._2
          val redisKey = "dbots:" + ip

          if (requests > 20) {
            redis.hset(redisKey, "requests", requests)
            redis.hset(redisKey, "added_time", System.currentTimeMillis() / 1000)
          } else {
            val added_time_optional = redis.hget(redisKey, "added_time")
            added_time_optional.map(added_time => {
              if (System.currentTimeMillis() / 1000 - added_time.toLong > 600)
                redis.del(redisKey)
            })
          }
        })

        sparkSession.createDataFrame(rdd.map(_._2._1).collect().map(event => {
          val optionBot = redis.hget("dbots:" + event.ip, "added_time")
          (event.ip, event.category_id, event.unix_time, event.`type`, optionBot.isDefined)
        })).toDF("ip", "category_id", "unix_time", "type", "is_bot").write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", "event_click")
          .option("table", "events_ds")
          .option("spark.cassandra.output.consistency.level", "ONE")
          .mode("APPEND")
          .save()

      })
  }

  def crateSparkEnvironment = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("NetworkWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/checkpoint-location/dstream")

    val sparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .config("spark.cassandra.connection.host", localhost)
      .withExtensions(new CassandraSparkExtensions)
      .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.output.consistency.level", "ONE")
      .getOrCreate()

    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "DStream_group_1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: lang.Boolean)
    )
    val topics = Array("user-click-data")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )
    (ssc, stream, sparkSession)
  }
}