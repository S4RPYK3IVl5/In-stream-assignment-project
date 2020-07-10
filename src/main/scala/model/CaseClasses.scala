package model
import spray.json.DefaultJsonProtocol._

object CaseClasses {
  case class Events(unix_time: Long, category_id: Int, ip: String, `type`: String)
  implicit val eventJsonFormat = jsonFormat4(Events)
}
