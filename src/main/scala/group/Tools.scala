package group

import scala.xml.NodeSeq

object Tools {

  private def duplicateFtest(groupNode:NodeSeq): Unit = {
    val features = (groupNode \ "feature_list" \ "feature" \ "fkey").map(_.text.trim)

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


  private def duplicateSchematest(groupNode:NodeSeq): Unit = {
    val schemas = (groupNode \ "schema_list" \ "schema")
      .map(_.text.trim)
      .map(x => x.split(",").toSet)

    println("============ XML group schema list ===============")

    schemas.map(x=>println(x))

    val duplicateSchemaKey = schemas
      .groupBy(x => x)
      .filter(x => x._2.length > 1)
      .map(_._1)
      .toList

    if (duplicateSchemaKey.length > 0) {
      throw new Exception(s"duplicate schema conf in xml file: ${duplicateSchemaKey}")
    }

  }


  private def aggregatorValidTest(groupNode:NodeSeq): Unit = {
    val aggTypes:Seq[String] = (groupNode \ "feature_list" \ "feature" \ "aggregator" \ "type")
      .map(_.text.trim).distinct

    println("============ XML feature aggregator type ===============")
    println(aggTypes)

    val invalidAggs = aggTypes.map{ aggType =>
      aggType match {
        case "count" => ("count", true)
        case "distinct_count" => ("distinct_count", true)
        case "ratio" => ("ratio", true)
        case "distinct_ratio" => ("distinct_ratio", true)
        case "stats" => ("stats", true)
        case _ => (s"invalidAggregator-${aggType}", false)
      }
    }.filter(x => x._2 == false).map(x => x._1).distinct.toList

    if(invalidAggs.length > 0) {
      throw new Exception(s"invalid aggregator type notices: ${invalidAggs}")
    }

  }


  private def aggregatorInfoTest(groupNode:NodeSeq): Unit = {
    val distinctXmlNode:NodeSeq = (groupNode \ "feature_list" \ "feature")
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

    val ratioXmlNode:NodeSeq = (groupNode \ "feature_list" \ "feature")
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

    val statsXmlNode:NodeSeq = (groupNode \ "feature_list" \ "feature")
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


    val disRatioXmlNode:NodeSeq = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "distinct_ratio")

    if(disRatioXmlNode.length > 0) {
      disRatioXmlNode.map { x =>
        val Array(fKey, space, distinctKey, topSpace, bottomSpace) =
          Array(x \ "fkey", x \ "space", x \ "aggregator" \ "distinctkey", x \ "aggregator" \ "top",
            x \ "aggregator" \ "bottom").map(_.text.trim)

        if(topSpace == "" || bottomSpace == "" || distinctKey == "") {
          throw new Exception(s"please config topSpace and bottomSpace and distinctKey in xml for ${fKey}")
        }
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

  def duplicateGroupNameTest(xmlFile:scala.xml.Elem): Unit = {
    val groupNames = (xmlFile \ "feature-group" \ "group")
      .map(_.text.trim)


    println("============ XML groupNames ===============")

    groupNames.map(x=>println(x))

    val duplicateGroupNames = groupNames
      .groupBy(x => x)
      .filter(x => x._2.length > 1)
      .map(_._1)
      .toList

    if (duplicateGroupNames.length > 0) {
      throw new Exception(s"duplicate group name conf in xml file: ${duplicateGroupNames}")
    }

  }


  def groupXmlConfCheck(groupNode: NodeSeq): Unit = {
    duplicateFtest(groupNode)
    duplicateSchematest(groupNode)
    aggregatorValidTest(groupNode)
    aggregatorInfoTest(groupNode)
  }

}
