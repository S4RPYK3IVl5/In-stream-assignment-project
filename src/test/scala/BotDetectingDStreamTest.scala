import dstream._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

class BotDetectingDStreamTest  extends AnyFlatSpec {

  // bot have ip 172.20.X.X
  val strData: String =
    """{"unix_time": 1596118430, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118430, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118430, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118430, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118431, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118431, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118431, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118431, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118432, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118432, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118432, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118432, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118433, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118433, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118433, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118433, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118434, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118434, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118434, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118434, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118435, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118435, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118435, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118435, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118436, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118436, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118436, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118436, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118437, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118437, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118437, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118437, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118438, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118438, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118438, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118438, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
      |{"unix_time": 1596118439, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118439, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118439, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
      |{"unix_time": 1596118439, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}""".stripMargin
  val listData: Array[String] = strData.split("\n")


  "A bot" should "be determined" in {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DStream test")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/Users/asaprykin/Documents/lpProjects/In-stream-assignment-project/file/checkpoint-location/dstream-test")

    val sparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val inputStream: InputDStream[String] = ssc.queueStream(inputData)
    inputData += sc.makeRDD(listData)

    val eventsStream = calculatingEvents(inputStream)
    eventsStream.map(_._2).foreachRDD(rdd => {
      val arrTupleOfIds = rdd.collect().partition(_._1.ip == "172.20.0.0")
      assert(arrTupleOfIds._1.forall(_._2 > 20))
      assert(arrTupleOfIds._2.forall(_._2 < 20))
    })

  }

}
