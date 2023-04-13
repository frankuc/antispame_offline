package feature

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq
import log._

case class DistinctItem(fKey: String, space: String, distinctKey: String) {
  override def toString: String = {
    s"${fKey}-${space}-${distinctKey}"
  }
}

class DistinctCountAggregator extends Aggregator {
  var items: Array[DistinctItem] = null
  var logInfo:LogInfo = null
  var logFactory:Log = null
  var outHiveTable:String = ""
  var date:String = ""
  var hour:String = ""
  var window:Int = 1
  var version:String = ""
  var dKey:String = ""
  var logSpace:String = ""
  var feaConf:String = ""
  var fKeys:String = ""

  var aggKeySqlQuerys:String = ""
  var dKeyToString:String = ""
  var groupName:String = ""
  var dKeyList:List[String] = List[String]()
  var mergeQueryStr:String = ""

  def initialize(groupNode: NodeSeq, args:ExecutorArgs) = {
    this.outHiveTable = args.outHiveTable
    this.date =  args.date
    this.hour = args.hour
    this.window = args.window
    this.version = args.version

    val aggKeys = (groupNode \ "feature" \ "dkey").map(_.text.trim).distinct.toArray

    aggKeySqlQuerys = aggKeys.map { aggKey:String =>

      val aggKeyFeatures = (groupNode \ "feature").filter(x => (x \ "dkey").text.trim == aggKey)
      val xmlNode:NodeSeq = aggKeyFeatures
        .filter(x => (x \ "aggregator" \ "type").text.trim == "distinct_count")

      items = xmlNode.map { x =>
        var Array(fKey, space, distinctKey) = Array(x \ "fkey", x \ "space",
          x \ "aggregator" \ "distinctkey").map(_.text.trim)

        if(space == "") {
          space = "TRUE"
        }

        DistinctItem(fKey, Utils.spaceParse(space), distinctKey)
      }.toArray

      feaConf = items.map { item =>
        val space: String = Utils.spaceParse(item.space)
        val fkey: String = item.fKey
        val distinctKey:String = item.distinctKey
        s"COUNT(DISTINCT(IF(${space}, concat_ws('|', ${distinctKey}), NULL))) AS ${fkey}"
      }.mkString(",\n\t\t    ")

      fKeys = items.map { item =>
        val fkey: String = item.fKey
        s"'${fkey}', \n\t\t ${fkey}"
      }.mkString(",\n\t\t")

      logInfo = Utils.basicInfo(groupNode)
      logSpace = logInfo.space

      dKey = aggKey.split(",").map(_.trim).mkString(",")
      dKeyToString = aggKey.split(",").map(_.trim).mkString("_")
      groupName = logInfo.groupName
      logFactory = LogFactory.get(logInfo.baseLog)
      dKeyList = dKeyList ++ List(dKeyToString)

      aggKeySqlQuery(dKeyToString)
    }.mkString(",\n")

    mergeQueryStr = dKeyList.map{ key =>
      s"""
         |SELECT  dkey,
         |        dval,
         |        fkey,
         |        fval
         |FROM
         |        ${key}_tab
         |""".stripMargin

    }.mkString("\n UNION ALL")

  }

  def aggKeySqlQuery(dKeyToString:String): String = {
    val query :String =
      s"""
         |${dKeyToString}_tab AS (
         |
         |SELECT  '${dKeyToString}' AS dkey,
         |        atb.dval AS dval,
         |        btb.fkey AS fkey,
         |        btb.fval AS fval
         |FROM    (
         |            SELECT  concat_ws('|', ${dKey}) AS dval,
         |                    ${feaConf}
         |            FROM    ${logFactory.logName}
         |            WHERE   ${logSpace}
         |            GROUP BY
         |                    ${dKey}
         |        ) atb
         |LATERAL VIEW
         |        EXPLODE (
         |            MAP(
         |                ${fKeys}
         |            )
         |        ) btb AS fkey,
         |        fval
         |)
         |""".stripMargin

    query
  }

  def sqlQuery(): String = {
    val writeSql:String =
      s"""
         |INSERT OVERWRITE TABLE ${outHiveTable} PARTITION (
         |    date = '${date}',
         |    hour = '${hour}',
         |    aggregator_type = 'distinct_count',
         |    group = '${groupName}',
         |    version = '${version}'
         |)
         |""".stripMargin

    val mergeQuery :String =
      s"""
         |SELECT  dkey,
         |        dval,
         |        fkey,
         |        fval
         |FROM    (
         |           ${mergeQueryStr}
         |        )
         |""".stripMargin

    val query :String = aggKeySqlQuerys + writeSql + "\n" + mergeQuery

    query
  }

  def compute(spark:SparkSession): Unit = {
    val log = logFactory.logString(date, hour, window)
    val query:String = log + ",\n" + sqlQuery()
    println(query)
    spark.sql(query)
  }

}