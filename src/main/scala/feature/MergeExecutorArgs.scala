package feature

class MergeExecutorArgs(argv: Array[String]) extends Serializable {
  var date: String = "20221220"
  var hour: String = "12"
  var mergeConfig: String = "merge.xml"
  var mergeGroups: Array[String] = Array[String]("DEVICE_CONVERT_FEATURE_MERGE")

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

    case ("--merge-config") :: value :: tail =>
      mergeConfig = value
      parse(tail)

    case ("--merge-group") :: StrArrayParam(value) :: tail =>
      mergeGroups = value
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
        "  --hour HOUR process date\n" +
        "  --merge-config xml file \n" +
        "  --merge-group which group need merge.")
    System.exit(exitCode)
  }
}
