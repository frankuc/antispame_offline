package group

private object StrArrayParam {
  def unapply(str: String): Option[Array[String]] = {
    Some(str.split(",").map(_.trim))
  }
}

case class InvalidArgException(smth: String) extends Exception

class GroupExecutorArgs(argv: Array[String]) extends Serializable {
  var date: String = "20230512"
  var hour: String = "23"
  var window: Int = 1
  var outHiveTable:String = "union_anti.ads_union_antispam_group_aggregator_feature_hourly"
  var GroupConfig: String = "group.xml"
  var GroupNames: Array[String] = Array[String]("CONVERT_GROUP_FEI_ZHU", "CONVERT_GROUP_FEI_ZHU_2")

  parse(argv.toList)

  private def parse(args: List[String]): Unit = args match {
    case ("--date") :: value :: tail =>
      date = value
      parse(tail)

    case ("--hour") :: value :: tail =>
      if (value.toInt >=0 && value.toInt < 24) {
        hour = value
        parse(tail)
      } else {
        System.err.println(s"Invalid input hour: $value")
        throw new InvalidArgException(s"Invalid input hour: $value")
      }

    case ("--window") :: value :: tail =>
      window = value.toInt
      parse(tail)

    case ("--group-config") :: value :: tail =>
      GroupConfig = value
      parse(tail)

    case ("--group_name") :: StrArrayParam(value) :: tail =>
      GroupNames = value
      parse(tail)

    case ("--out-table") :: value :: tail =>
      outHiveTable = value.trim
      parse(tail)

    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil =>

    case x =>
      System.err.println(s"Unknown argument: $x")
      throw new InvalidArgException(s"Unknown argument: $x")
  }

  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Worker [options]\n" +
        "\n" +
        "Options:\n" +
        "  --date DATE process date\n" +
        "  --hour HOUR process hour 0-23\n" +
        "  --group-config CONFIG_XML \n" +
        "  --group-name group names \n" +
        "  --out-table output hive table")
    System.exit(exitCode)
  }
}
