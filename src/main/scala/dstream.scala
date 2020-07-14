import java.sql.Timestamp

import com.redis.RedisClient
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.collection.mutable.Map
import utils._

object dstream {

  val localhost = "127.0.0.1"
  val redis = new RedisClient(localhost, 6379)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/checkpoint-location/dstream")

    val kafkaParam = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "DStream_group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    val topics = Array("user-click-data")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    def accumulateMappingFunction (key: (Timestamp, Timestamp, String),
                                   value: Option[Int],
                                   state: State[Int]): ((Timestamp, Timestamp, String), Int)  = {
      value match {
        case Some(x) => state.update(state.getOption().getOrElse(0) + x); (key, state.get)
        case None => (key, 0)
      }
    }

    stream
      .map(_.value)
      .map(x => parseString(x))
      .filter(_.nonEmpty)
      .map(a => (createTriple(a(0).toDouble, a(2)), 1))
      .mapWithState(StateSpec.function(accumulateMappingFunction _))
      .foreachRDD(rdd => {
        rdd.reduceByKey((x, y) => if (x > y) x else y)
          .collect().foreach( value => {
            val ip = value._1._3
            val requests = value._2
            val redisKey = "dbots:"+ip

            if (requests > 20) {
              println(ip + "      " + requests)
              redis.hset(redisKey, "requests", requests)
              redis.hset(redisKey, "added_time", System.currentTimeMillis()/1000)
            } else {
              println(ip + "      " + requests)
              val added_time_optional = redis.hget(redisKey, "added_time")
              added_time_optional.map(added_time => {
                if (System.currentTimeMillis()/1000 - added_time.toLong > 600)
                  redis.del(redisKey)
              })
            }
          })
      })

    ssc.start
    ssc.awaitTermination

  }
}
