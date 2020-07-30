import java.io.{BufferedWriter, File, FileWriter}

import com.datastax.oss.driver.api.core.CqlSessionBuilder
import dstream._
import org.scalatest.flatspec.AnyFlatSpec

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
               |{"unix_time": 1594906470, "category_id": 1009, "ip": "172.20.0.0", "type": "click"},
               |{"unix_time": 1594917582, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}]""".stripMargin

  /*
  * Hint: author of this test understand, that this test represent worst pattern of writing test,
  *  but it does what it has to do.
  */
  "A bot" should "be found in Redis inside DStream" in {

    val (ssc, stream, sparkSession) = crateSparkEnvironment

    computingData(stream, sparkSession)

    new Thread(new Runnable {
      override def run(): Unit = {
        val file = new File("file/data/data.json")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))
        var x = 0
        @scala.annotation.tailrec
        def loop(){
          bw.append(
            s"""{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
               |""".stripMargin)
          bw.flush()
          x = x + 1
          if (x == 20) return
          Thread.sleep(1000)
          loop()
        }
        loop()
      }
    }).start()

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(20000)
        closeStreaming(ssc)

        assert(redis.hget("dbots:" + "172.20.0.0", "requests").isDefined)
      }
    }).start()

    ssc.start
    ssc.awaitTermination

  }

  /*
  * Hint: author of this test understand, that this test represent worst pattern of writing test,
  *  but it does what it has to do.
  */
  "A bot" should "be found in Cassandra inside DStream" in {

    val (ssc, stream, sparkSession) = crateSparkEnvironment

    computingData(stream, sparkSession)

    new Thread(new Runnable {
      override def run(): Unit = {
        val file = new File("file/data/data.json")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))
        var x = 0
        @scala.annotation.tailrec
        def loop(){
          bw.append(
            s"""{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.20.0.0", "type": "click"}
               |{"unix_time": ${System.currentTimeMillis/1000}, "category_id": 1009, "ip": "172.10.0.0", "type": "click"}
               |""".stripMargin)
          bw.flush()
          x = x + 1
          if (x == 20) return
          Thread.sleep(1000)
          loop()
        }
        loop()
      }
    }).start()

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(20000)
        closeStreaming(ssc)

        val session = new CqlSessionBuilder().build()
        val rs = session
          .execute("SELECT * FROM event_click.events_ds WHERE is_bot=True ALLOW FILTERING;")
        val row = rs.one
        assert(row.getString("ip") === "172.20.0.0")
        session.close()
      }
    }).start()

    ssc.start
    ssc.awaitTermination

  }

}
