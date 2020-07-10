package model

import java.sql.Timestamp

object CaseClasses {
  case class Events(unix_time: Timestamp, category_id: Int, ip: String, `type`: String)
}
