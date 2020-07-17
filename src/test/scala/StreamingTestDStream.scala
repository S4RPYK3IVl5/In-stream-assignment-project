import java.io.{BufferedWriter, File, FileWriter}

import org.scalatest.flatspec.AnyFlatSpec
import dstream._

import scala.tools.nsc.classpath.FileUtils

class StreamingTestDStream extends AnyFlatSpec{

  // bots have ip 172.20.X.X
  val data = """[{"unix_time": 1594906460, "category_id": 1006, "ip": "172.10.0.2", "type": "view"},
               |{"unix_time": 1594906460, "category_id": 1000, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906460, "category_id": 1011, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906460, "category_id": 1014, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906461, "category_id": 1003, "ip": "172.10.0.0", "type": "view"},
               |{"unix_time": 1594906461, "category_id": 1016, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906461, "category_id": 1000, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906461, "category_id": 1012, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906462, "category_id": 1001, "ip": "172.10.0.0", "type": "view"},
               |{"unix_time": 1594906462, "category_id": 1019, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906462, "category_id": 1005, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906463, "category_id": 1017, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906463, "category_id": 1005, "ip": "172.10.0.1", "type": "click"},
               |{"unix_time": 1594906464, "category_id": 1008, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906464, "category_id": 1011, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906464, "category_id": 1000, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906465, "category_id": 1001, "ip": "172.10.0.1", "type": "view"},
               |{"unix_time": 1594906465, "category_id": 1002, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906465, "category_id": 1010, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906465, "category_id": 1017, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906466, "category_id": 1005, "ip": "172.10.0.1", "type": "view"},
               |{"unix_time": 1594906466, "category_id": 1017, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906466, "category_id": 1007, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906466, "category_id": 1008, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906467, "category_id": 1004, "ip": "172.10.0.2", "type": "view"},
               |{"unix_time": 1594906467, "category_id": 1001, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906467, "category_id": 1012, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906468, "category_id": 1017, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906468, "category_id": 1006, "ip": "172.10.0.1", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1014, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1001, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906469, "category_id": 1008, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1006, "ip": "172.10.0.0", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1013, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1011, "ip": "172.20.0.0", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1003, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906469, "category_id": 1002, "ip": "172.10.0.2", "type": "view"},
               |{"unix_time": 1594906469, "category_id": 1000, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906470, "category_id": 1007, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594906470, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}]
               |{"unix_time": 1594906471, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}]""".stripMargin

  "A bot" should "be found in Redis and Cassandra inside DStream" in {

    val file = new File("file/data/data.json")
    file.createNewFile()
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(data)
    bw.close()

    val (ssc, stream, sparkSession) = createSparkEnvironment

    val streamToWrite = prepareRDDToWrite(stream)

    writingData(streamToWrite, sparkSession)

    ssc.start
    ssc.awaitTermination
    ssc.stop()

    assert(redis.hget("dbots:" + "172.20.0.0", "requests") === Some("30"))

  }

}
