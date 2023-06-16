package group


class GroupPatternMatchArgs(argv: Array[String]) extends Serializable {
  var date: String = "20230512"
  var strategyDate:String = "20230525"
  var outHiveTable:String = "union_anti.ads_union_antispam_group_strategy_mining_anomaly_pattern_daily"
  var GroupConfig: String = "group.xml"
  var GroupNames: Array[String] = Array[String]("CONVERT_GROUP_FEI_ZHU", "CONVERT_GROUP_FEI_ZHU_2")

  parse(argv.toList)

  private def parse(args: List[String]): Unit = args match {
    case ("--date") :: value :: tail =>
      date = value
      parse(tail)

    case ("--strategy_date") :: value :: tail =>
      strategyDate = value
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
