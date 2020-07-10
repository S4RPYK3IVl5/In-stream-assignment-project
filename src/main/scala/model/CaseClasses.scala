package model

object CaseClasses {
  case class Events(unix_time: Long, category_id: Int, ip: String, `type`: String)
}
