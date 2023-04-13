package feature

import scala.xml.NodeSeq
import scala.xml.Elem

case class LogInfo(groupName:String, baseLog:String, window:Int, space: String) {
  override def toString: String = {
    s"${groupName}-${baseLog}-${window}-${space}"
  }
}

object Utils {
  def spaceParse(space: String): String = {
    var parsedSpace: String = space
    if (space == "") {
      parsedSpace = "TRUE"
    } else {
      if (space.contains(" gt ")) {
        parsedSpace = space.replaceAll(" gt ", " > ")
      }
      if (space.contains(" ge ")) {
        parsedSpace = space.replaceAll(" ge ", " >= ")
      }
      if (space.contains(" lt ")) {
        parsedSpace = space.replaceAll(" lt ", " < ")
      }
      if (space.contains(" le ")) {
        parsedSpace = space.replaceAll(" le ", " <= ")
      }
    }
    parsedSpace
  }

  def basicInfo(groupNode: NodeSeq): LogInfo = {
    val baseLog: String = (groupNode \ "log").text.trim
    val groupName: String = (groupNode \ "group").text.trim

    val window: Int = (groupNode \ "window").text.trim.toInt
    var space: String = (groupNode \ "space").text.trim
    if(space == "") {
      space = "TRUE"
    }

    LogInfo(groupName, baseLog, window, spaceParse(space))
  }

  def iatFuncMatch(func: String, key: String, space:String): String = {
    val iatFuncStr = func match {
      case "minus" => s"b.${key} - a.${key}"
      case "minus_abs" => s"abs(b.${key} - a.${key})"
      case "div" => s"b.${key} / a.${key}"
      case "div_max" => s"IF(b.${key} > a.${key}, b.${key} / a.${key}, a.${key} / b.${key})"
      case "div_min" => s"IF(b.${key} < a.${key}, b.${key} / a.${key}, a.${key} / b.${key})"
      case _ => {
        println("func is invalid!");
        sys.exit(1)
      }
    }

    s"IF(b.${space} AND a.${space}, ${iatFuncStr}, NULL) AS iat_${key}_${func}"

  }

  def iatFuncMatch(func:String, key:String, space:String, truncateVal:Float):String = {
    val iatFuncStr = func match {
      case "minus" => s"b.${key} - a.${key}"

      case "minus_abs" => s"abs(b.${key} - a.${key})"
      case "div" => s"b.${key} / a.${key}"
      case "div_max" => s"IF(b.${key} > a.${key}, b.${key} / a.${key}, a.${key} / b.${key})"
      case "div_min" => s"IF(b.${key} < a.${key}, b.${key} / a.${key}, a.${key} / b.${key})"
      case _ => {
        println("func is invalid!");
        sys.exit(1)
      }
    }

    s"IF(b.${space} AND a.${space}, IF(${iatFuncStr} IS NULL OR ${iatFuncStr} <= ${truncateVal}, ${iatFuncStr}, ${truncateVal}), NULL) AS iat_${key}_${func}"
  }

  def feaAggMatch(fkey:String, aggKey:String, space:String, xmlNode:NodeSeq):String = {

    val aggType = (xmlNode \ "type").text.trim
    val fspace = space.replaceAll("iat", aggKey)
    var feaLine = ""

    aggType match {
      case "stats" => {
        val func = (xmlNode \ "func").text.trim.toUpperCase
        feaLine = s"${func}(IF(${fspace}, ${aggKey}, NULL)) AS ${fkey}"
      }
      case "ratio" => {
        val topSpace = spaceParse((xmlNode \ "top").text.trim).replaceAll("iat", aggKey)
        val bottomSpace = spaceParse((xmlNode \ "bottom").text.trim).replaceAll("iat", aggKey)
        feaLine = s"SUM(IF(${fspace} AND ${bottomSpace} AND ${topSpace}, 1, 0)) / SUM(IF(${fspace} AND ${bottomSpace}, 1, 0)) AS ${fkey}"
      }
      case "rank_ratio" => {
        val topSpace = spaceParse((xmlNode \ "top").text.trim).replaceAll("iat", aggKey)
        val bottomSpace = spaceParse((xmlNode \ "bottom").text.trim).replaceAll("iat", aggKey)
        feaLine = s"MIN(IF(${fspace} AND ${bottomSpace} AND ${topSpace}, rank_num, NULL)) / MAX(IF(${fspace} AND ${bottomSpace}, rank_num, NULL)) AS ${fkey}"
      }
      case _ => {
        println("aggType is invalid!");
        sys.exit(1)
      }
    }

    feaLine
  }

  private def duplicateFtest(groupNode:NodeSeq): Unit = {
    var features = (groupNode \ "feature" \ "fkey").map(_.text.trim)
    var iatFeatures:Seq[String] = null

    val iatFeaXml = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "iat")

    if(iatFeaXml.length > 0) {
      iatFeaXml.map { x =>
        iatFeatures = (x \ "valkey" \ "iatconf" \ "iatfea" \ "iatfkey").map(_.text.trim)
        features = features ++ iatFeatures
      }
    }

    println("============ XML feature list ===============")

    features.map(x=>println(x))

    val duplicateFKey = features
      .groupBy(x => x)
      .filter(x => x._2.length > 1)
      .map(_._1)
      .toList

    if (duplicateFKey.length > 0) {
      throw new Exception(s"duplicate fkey conf in xml file: ${duplicateFKey}")
    }

  }

  private def aggregatorValidTest(groupNode:NodeSeq): Unit = {
    val aggTypes:Seq[String] = (groupNode \ "feature" \ "aggregator" \ "type")
      .map(_.text.trim).distinct

    println("============ XML aggregator type ===============")
    println(aggTypes)
    println("============ XML aggregator type ===============")

    val invalidAggs = aggTypes.map{ aggType =>
      aggType match {
        case "count" => ("count", true)
        case "distinct_count" => ("distinct_count", true)
        case "ratio" => ("ratio", true)
        case "top_n_ratio" => ("top_n_ratio", true)
        case "stats" => ("stats", true)
        case "iat" => ("iat", true)
        case _ => (s"invalidAggregator-${aggType}", false)
      }
    }.filter(x => x._2 == false).map(x => x._1).distinct.toList

    if(invalidAggs.length > 0) {
      throw new Exception(s"invalid notices: ${invalidAggs}")
    }

  }

  private def aggregatorInfoTest(groupNode:NodeSeq): Unit = {
    val distinctXmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "distinct_count")

    if(distinctXmlNode.length > 0) {
      distinctXmlNode.map { x =>
        val Array(fKey, space, distinctKey) = Array(x \ "fkey", x \ "space",
          x \ "aggregator" \ "distinctkey").map(_.text.trim)
        if(distinctKey == "") {
          throw new Exception(s"please config distinctKey in xml for ${fKey}")
        }
      }
    }

    val ratioXmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "ratio")

    if(ratioXmlNode.length > 0) {
      ratioXmlNode.map { x =>
        val Array(fKey, space, topSpace, bottomSpace) =
          Array(x \ "fkey", x \ "space", x \ "aggregator" \ "top",
            x \ "aggregator" \ "bottom").map(_.text.trim)

        if(topSpace == "" || bottomSpace == "") {
          throw new Exception(s"please config topSpace and bottomSpace in xml for ${fKey}")
        }
      }
    }

    val statsXmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "stats")

    if(statsXmlNode.length > 0) {
    statsXmlNode.map { x =>
      val Array(fKey, space, func, statsKey, truncate) = Array(
        x \ "fkey",
        x \ "space",
        x \ "aggregator" \ "func",
        x \ "aggregator" \ "statskey",
        x \ "aggregator" \ "truncate"
      ).map(_.text.trim)

       if(func == "" || statsKey == "") {
        throw new Exception(s"please config 'func' and 'statsKey' in xml for ${fKey}")
       }
     }

    }

    val topNXmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "top_n_ratio")
    if(topNXmlNode.length > 0) {
    topNXmlNode.map { x =>
        val Array(fKey, space, cKey, n) = Array(
        x \ "fkey",
        x \ "space",
        x \ "aggregator" \ "ckey",
        x \ "aggregator" \ "num"
      ).map(_.text.trim)

      if(cKey == "" || n == "") {
        throw new Exception(s"please config 'cKey' and 'n' in xml for ${fKey}")
      }
    }
    }


    val iatXmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "iat")
    if(iatXmlNode.length > 0) {
    val iatFea:NodeSeq = iatXmlNode(0)

    val sortkey = (iatFea \ "aggregator" \ "skey").map(_.text.trim)
    val valKeys = (iatFea \ "valkey" \ "key").map(_.text.trim)

    if(sortkey.contains("")  || valKeys.contains("")) {
      throw new Exception(s"check whether 'skey' or valkey`s 'key' is null in iat feature xml config")
    }

    }

  }

  def featureGroupCheck(xmlFile:scala.xml.Elem, featureGroupArr:Array[String]): Unit = {
    featureGroupArr.map { groupName =>
      val groupNode = (xmlFile \ "feature-group").filter(x => (x \ "group").text == groupName)
      if(groupNode.size == 0) {
        throw new Exception(s"invalid match notices ! feature-group: ${groupName} is not match in xml feature configuration")
      }
    }
  }

  def featureXmlConfCheck(groupNode:NodeSeq): Unit = {
    duplicateFtest(groupNode)
    aggregatorValidTest(groupNode)
    aggregatorInfoTest(groupNode)
  }

  def mergeXmlConfCheck(groupNode:NodeSeq) :Unit = {
    var Array(inputTable, group, date,
    hour, dKey, aggType, version) = Array(
      groupNode \ "input-tab",
      groupNode \ "group",
      groupNode \ "date",
      groupNode \ "hour",
      groupNode \ "dkey",
      groupNode \ "aggtype",
      groupNode \ "version",
    ).map(_.text.trim)


    if(inputTable == "") {
      throw new Exception(s" input-tab is null in XML CONFIG")
    }

    if(group == "") {
      throw new Exception(s" group is null in XML CONFIG")
    }

    if(dKey == "") {
      throw new Exception(s" dkey is null in XML CONFIG")
    }

    if(aggType == "") {
      throw new Exception(s" aggtype is null in XML CONFIG")
    }

    if(version == "") {
      throw new Exception(s" version is null in XML CONFIG")
    }

    val Array(outTab, hdfsPath) = Array(
      groupNode \ "output" \ "tab",
      groupNode \ "output" \ "hdfs"
    ).map(_.text.trim)

    if(outTab == "" && hdfsPath == "") {
       throw new Exception(s" output-tab & output-hdfs is both null in XML CONFIG")
    }

    (groupNode \ "feature").map { feaNode =>
      val Array(fkey, nvf, truncate, func) = Array(
        feaNode  \ "fkey",
        feaNode  \ "nvf",
        feaNode \ "truncate",
        feaNode  \ "func"
      ).map(_.text.trim)

      println(fkey + "|" + nvf + "|" + truncate + "|" + func)
    }

  }

}