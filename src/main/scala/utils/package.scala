import java.sql.Timestamp

package object utils {

  def parseString(str: String): List[String] = {
    val keyValueArr = str.asInstanceOf[String].replace("[", "").replace("]","")
      .replace("\"", "").replace(" ", "")
      .replace("{", "").replace("}", "")
      .split(",")
    if (keyValueArr.size < 4) Nil
    else if (!keyValueArr.forall(_.split(":").length == 2)) Nil
    else keyValueArr.map(x => x.split(":")(1) ).toList
  }

  def createTriple(timestamp: Double, ip: String): (Timestamp, Timestamp, String) = {
    val floor = (timestamp/10).floor
    val ceil = (timestamp/10).ceil
    if (floor == ceil)
      (new Timestamp((floor*10000).toLong), new Timestamp(((ceil+0.9)*10000).toLong), ip)
    else
      (new Timestamp((floor*10000).toLong), new Timestamp(((ceil-0.1)*10000).toLong), ip)
  }

}
