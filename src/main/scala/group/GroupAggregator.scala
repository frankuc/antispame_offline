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

    schemaItems = (groupNode \ "schema_list" \ "schema")
      .map(_.text.trim)
      .map{schemaStr => SchemaItem(schemaStr)}
      .toArray

    println(schemaItems)

    val countNodes = (groupNode \ "feature_list" \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "count")

    countItems = countNodes.map{ x =>
      var Array(fKey, space) = Array(x \ "fkey", x \ "space").map(_.text.trim)
      if(space == "") {
        space = "TRUE"
      }
      CountingItem(fKey, Utils.spaceParse(space))
    }.toArray

    println(countItems)


  }

  def sqlQuery(): String = {

    ""
  }
  def compute(spark:SparkSession): Unit = {
    println("hello compute()")
  }

}
