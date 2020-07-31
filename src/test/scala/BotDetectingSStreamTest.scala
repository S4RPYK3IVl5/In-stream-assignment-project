import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import sstream._

class BotDetectingSStreamTest extends AnyFlatSpec {

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

    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("bot-detection")
      .getOrCreate()

    val rdd = sparkSession.sparkContext.parallelize(listData)
    val rowRdd = rdd.map(v => Row(v))
    val df = sparkSession.createDataFrame(rowRdd, StructType(StructField("value", StringType, true)::Nil))

    val valueDs = convertDataToEventsDS(df, sparkSession: SparkSession)
    val windowedDf = windowData(valueDs, sparkSession)

    val arrTupleOfIds = windowedDf.collect().partition(_.getAs[String]("ip") == "172.20.0.0")
    assert(arrTupleOfIds._1.forall(_.getAs[Long]("count") > 20))
    assert(arrTupleOfIds._2.forall(_.getAs[Long]("count") < 20))

  }

}
