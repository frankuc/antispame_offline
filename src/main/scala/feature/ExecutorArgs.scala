package feature

private object StrArrayParam {
  def unapply(str: String): Option[Array[String]] = {
    Some(str.split(",").map(_.trim))
  }
}

case class InvalidArgException(smth: String) extends Exception

class ExecutorArgs(argv: Array[String]) extends Serializable {
  var date: String = "20221220"
  var hour: String = "12"
  var window: Int = 1
  var outHiveTable:String = "union_anti.dwa_union_antispam_dim_aggregator_feature_hourly_new"
  var version:String = "test_1.0"
  var featureConfig: String = "features.xml"
  var featureGroup: Array[String] = Array[String]("DEVICE_CONVERT_FEATURE", "APP_CONVERT_FEATURE")

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

    case ("--feature-config") :: value :: tail =>
      featureConfig = value
      parse(tail)

    case ("--feature-group") :: StrArrayParam(value) :: tail =>
      featureGroup = value
      parse(tail)

    case ("--out-table") :: value :: tail =>
      outHiveTable = value.trim
      parse(tail)

    case ("--version") :: value :: tail =>
      version = value.trim
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
        "  --feature-config CONFIG_XML \n" +
        "  --out-table output hive table \n" +
        "  --version project version.")
    System.exit(exitCode)
  }
}
