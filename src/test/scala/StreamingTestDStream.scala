import java.io.{BufferedWriter, File, FileWriter}

import com.datastax.oss.driver.api.core.CqlSessionBuilder
import dstream._
import org.scalatest.flatspec.AnyFlatSpec

class StreamingTestDStream extends AnyFlatSpec{

  val (ssc, stream, sparkSession) = crateSparkEnvironment
  computingData(stream, sparkSession)

  // bot have ip 172.20.X.X
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

  /*
  * Hint: author of this test understand, that this test represent worst pattern of writing test,
  *  but it does what it has to do.
  */
  "A bot" should "be found in Redis and Cassandra inside DStream" in {

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(20000)
        closeStreaming(ssc)

        // Ensure that bot cached in Redis
        assert(redis.hget("dbots:" + "172.20.0.0", "requests").isDefined)

        val session = new CqlSessionBuilder().build()
        val rs = session
          .execute("SELECT * FROM event_click.events_ds WHERE is_bot=True ALLOW FILTERING;")
        val row = rs.one
        //Ensure that bot event marked as is_bot=true in Cassandra
        assert(row.getString("ip") === "172.20.0.0")
        session.close()
      }
    }).start()

    ssc.start
    ssc.awaitTermination

  }

}
