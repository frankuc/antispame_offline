package feature

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq
import log._

case class TopNRatioItem(fKey: String,
                         space: String,
                         cKey: String,
                         n: Int) {
  override def toString: String = {
    s"${fKey}-${space}-${cKey}-${n}"
  }
}

class TopNRatioAggregator extends Aggregator {
  var items: Array[TopNRatioItem] = null
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

  var partCnts:String = ""
  var rankConfs:String = ""
  var partKeyStr:String = ""
  var cntConfs:String = ""
  var partKeys:String = ""

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
        .filter(x => (x \ "aggregator" \ "type").text.trim == "top_n_ratio")

      items = xmlNode.map { x =>
        val Array(fKey, space, cKey, n) = Array(
          x \ "fkey",
          x \ "space",
          x \ "aggregator" \ "ckey",
          x \ "aggregator" \ "num"
        ).map(_.text.trim)

        val spaceStr = if(space == "" ) "TRUE" else space

        TopNRatioItem(fKey, Utils.spaceParse(spaceStr), cKey, n.toInt)
      }.toArray

      partKeys = items.map { item =>
        val cKey: String = item.cKey
        val cKeyString: String = item.cKey
          .split(",").mkString("_")
        s"concat_ws('|', ${cKey}) AS ${cKeyString}_partKey"
      }.mkString(",")

      partKeyStr = items.map { item =>
        val cKeyString = item.cKey
          .split(",").mkString("_")

        s"${cKeyString}_partKey"
      }.mkString(",\n")

      cntConfs = items.map { item =>
        val space: String = item.space
        val cKeyString: String = item.cKey
          .split(",").mkString("_")

        s"COUNT(dval, IF(${space}, ${cKeyString}_partKey, NULL)) OVER(PARTITION BY dval, ${cKeyString}_partKey) AS ${cKeyString}_cnt"
      }.mkString(",\n")

      partCnts = items.map { item =>
        val cKeyString: String = item.cKey
          .split(",").mkString("_")
        s"${cKeyString}_cnt"
      }.mkString(",\n")

      rankConfs = items.map { item =>
        val cKeyString: String = item.cKey
          .split(",").mkString("_")

        s"DENSE_RANK() OVER (PARTITION BY dval ORDER BY ${cKeyString}_cnt DESC, ${cKeyString}_partKey DESC) AS ${cKeyString}_cnt_rank"
      }.mkString(",\n")


      feaConf = items.map { item =>
        val fkey: String = item.fKey
        val n:Int = item.n
        val cKeyString: String = item.cKey
          .split(",").mkString("_")

        s"SUM(IF(${cKeyString}_cnt > 0 AND ${cKeyString}_cnt_rank <= ${n}, 1, 0)) / SUM(IF(${cKeyString}_cnt > 0, 1, 0)) AS ${fkey}"
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


    mergeQueryStr = dKeyList.map { key =>
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
    val query:String =
      s"""
         |${dKeyToString}_rank_tb AS (
         |    SELECT  dval,
         |            ${partCnts},
         |            ${rankConfs}
         |    FROM    (
         |                SELECT  dval,
         |                        ${partKeyStr},
         |                        ${cntConfs}
         |                FROM    (
         |                            SELECT  concat_ws('|', ${dKey}) AS dval,
         |                                    ${partKeys},
         |                                    *
         |                            FROM    ${logFactory.logName}
         |                            WHERE   ${logSpace}
         |                        )
         |            )
         |),
         |
         |${dKeyToString}_tab AS (
         |
         |SELECT  '${dKeyToString}' AS dkey,
         |        atb.dval AS dval,
         |        btb.fkey AS fkey,
         |        btb.fval AS fval
         |FROM    (
         |            SELECT  dval,
         |                    ${feaConf}
         |            FROM    ${dKeyToString}_rank_tb
         |            GROUP BY
         |                    dval
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
         |    aggregator_type = 'top_n_ratio',
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
