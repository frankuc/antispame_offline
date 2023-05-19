package group

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq
import feature.Utils

case class CountingItem(fKey: String, space: String) {
  override def toString: String = {
    s"${fKey}-${space}"
  }
}

case class DistinctItem(fKey: String, space: String, distinctKey: String) {
  override def toString: String = {
    s"${fKey}-${space}-${distinctKey}"
  }
}

case class RatioItem(fKey: String, space: String,
                     topSpace: String, bottomSpace: String) {
  override def toString: String = {
    s"${fKey}-${space}-${topSpace}-${bottomSpace}"
  }
}


case class DisRatioItem(fKey: String, space: String, distinctKey: String,
                     topSpace: String, bottomSpace: String) {
  override def toString: String = {
    s"${fKey}-${space}-${distinctKey}-${topSpace}-${bottomSpace}"
  }
}

case class StatsItem(fKey: String,
                     space: String,
                     func: String,
                     statsKey: String,
                     truncate: Float) {
  override def toString: String = {
    s"${fKey}-${space}-${func}-${statsKey}-${truncate.toString}"
  }
}

case class SchemaItem(fields:String) {
  override def toString: String = {
    s"${fields}"
  }
}

class GroupAggregator extends Serializable {
  var outHiveTable:String = ""
  var date:String = ""
  var hour:String = ""
  var window:Int = 1

  var countItems: Array[CountingItem] = null
  var disCountItems: Array[DistinctItem] = null
  var ratioItems: Array[RatioItem] = null
  var disRatioItems: Array[DisRatioItem] = null
  var statsItems: Array[StatsItem] = null
  var schemaItems: Array[SchemaItem] = null


  def initialize(groupNode: NodeSeq, args:GroupExecutorArgs) = {
    this.outHiveTable = args.outHiveTable
    this.date =  args.date
    this.hour = args.hour
    this.window = args.window

    val schemaNodes = (groupNode \ "schema_list" \ "schema").map(_.text.trim)
    if(schemaNodes.size > 0 ) {
      schemaItems = schemaNodes
        .map{schemaStr => SchemaItem(schemaStr)}
        .toArray
      println(schemaItems.mkString("|"))
    }


    val countNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "count")

    if(countNodes.size > 0) {
      countItems = countNodes.map { x =>
        var Array(fKey, space) = Array(x \ "fkey", x \ "space").map(_.text.trim)
        if(space == "") {
          space = "TRUE"
        }
        CountingItem(fKey, Utils.spaceParse(space))
      }.toArray

      println(countItems.mkString("|"))
    }


    val disCountNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "distinct_count")

    if(disCountNodes.size > 0) {
      disCountItems = disCountNodes.map { x =>
        var Array(fKey, space, distinctKey) = Array(x \ "fkey", x \ "space",
          x \ "aggregator" \ "distinctkey").map(_.text.trim)

        if(space == "") {
          space = "TRUE"
        }
        DistinctItem(fKey, Utils.spaceParse(space), distinctKey)
      }.toArray

      println(disCountItems.mkString("|"))

    }

    val ratioNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "ratio")

    if(ratioNodes.size > 0) {

      ratioItems = ratioNodes.map { x =>

        val Array(fKey, space, topSpace, bottomSpace) =
          Array(x \ "fkey", x \ "space", x \ "aggregator" \ "top",
            x \ "aggregator" \ "bottom").map(_.text.trim)

        val spaceStr = if(space == "") "TRUE" else space

        val topSpaceStr = if(topSpace == "") "TRUE" else topSpace

        val bottomSpaceStr = if(bottomSpace == "") "TRUE" else bottomSpace

        RatioItem(fKey, Utils.spaceParse(spaceStr),
          Utils.spaceParse(topSpaceStr), Utils.spaceParse(bottomSpaceStr))

      }.toArray

      println(ratioItems.mkString("|"))

    }


    val disRatioNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "distinct_ratio")

      if(disRatioNodes.size > 0) {
        disRatioItems = disRatioNodes.map { x =>

        val Array(fKey, space, distinctkey, topSpace, bottomSpace) =
          Array(x \ "fkey", x \ "space", x \ "distinctkey", x \ "aggregator" \ "top",
            x \ "aggregator" \ "bottom").map(_.text.trim)

        val spaceStr = if(space == "") "TRUE" else space

        val topSpaceStr = if(topSpace == "") "TRUE" else topSpace

        val bottomSpaceStr = if(bottomSpace == "") "TRUE" else bottomSpace

        DisRatioItem(fKey, Utils.spaceParse(spaceStr), distinctkey,
          Utils.spaceParse(topSpaceStr), Utils.spaceParse(bottomSpaceStr))
       }.toArray

        println(disRatioItems.mkString("|"))
      }


    val statsNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "stats")

    if(statsNodes.size > 0) {

      statsItems = statsNodes.map { x =>
        val Array(fKey, space, func, statsKey, truncate) = Array(
          x \ "fkey",
          x \ "space",
          x \ "aggregator" \ "func",
          x \ "aggregator" \ "statskey",
          x \ "aggregator" \ "truncate"
        ).map(_.text.trim)

        val spaceStr = if(space == "" ) "TRUE" else space

        StatsItem(
          fKey,
          Utils.spaceParse(spaceStr),
          func.toUpperCase,
          statsKey,
          if(truncate != "") truncate.toFloat else -1.0F)

      }.toArray

      println(statsItems.mkString("|"))

    }






  }

  def sqlQuery(): String = {

    ""
  }

  def compute(spark:SparkSession): Unit = {
    println("hello compute()")
  }

}
