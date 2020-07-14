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

}
