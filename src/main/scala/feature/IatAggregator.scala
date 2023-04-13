package feature

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq
import log._

case class dsItem(dkey:String, skey:String) {
  override def toString: String = {
    s"${dkey}-${skey}"
  }
}

case class feaItem(fkey:String,
                   space:String,
                   agg:NodeSeq) {
  override def toString: String = {
    s"${fkey}-${space}-${agg.toString()}"
  }
}

case class IatConfItem(func:String,
                       space:String,
                       iattruncate:Float,
                       feaArr:Array[feaItem]) {
  override def toString: String = {
    s"${func}-${space}-${iattruncate.toString}-${feaArr.map(x=>x.toString()).mkString(",")}"
  }
}

case class ValKeyItem(key:String,
                      truncate:Float,
                      space:String,
                      iatConfArr:Array[IatConfItem]
                     ) {
  override def toString: String = {
    s"${key}-${truncate.toString}-${space}-${iatConfArr.map(x=>x.toString()).mkString(",")}"
  }
}

class IatAggregator extends Aggregator {
  var logInfo:LogInfo = null
  var logFactory:Log = null
  var dKey:String = ""
  var outHiveTable:String = ""
  var date:String = ""
  var hour:String = ""
  var window:Int = 1
  var version:String = ""
  var logSpace:String = ""
  var fieldKeys:String = ""
  var sortKey:String = ""
  var fKeys:String = ""

  var ValKeyItems: Array[ValKeyItem] = null
  var ValKeys: String = ""
  var IatConfItems: Array[IatConfItem] = null
  var iatConfs: String = ""
  var feaConfs: String = ""

  var dsKeyArr: Array[dsItem] = null
  var groupName:String = ""
  var aggKeySqlQuerys:String = ""
  var dimKeyStr:String = ""
  var fieldKeyList:List[String] = List[String]()
  var iatRankSqlStr:String = ""
  var allFieldKeys:String = ""
  var allRankIds:String = ""
  var mergeQueryStr:String = ""

  def initialize(groupNode: NodeSeq, args:ExecutorArgs) = {

    this.outHiveTable = args.outHiveTable
    this.date =  args.date
    this.hour = args.hour
    this.window = args.window
    this.version = args.version

    val xmlNode:NodeSeq = (groupNode \ "feature")
      .filter(x => (x \ "aggregator" \ "type").text.trim == "iat")

    dsKeyArr = xmlNode.map { x =>
      val Array(dkey, skey) = Array(
        x \ "dkey",
        x \ "aggregator" \ "skey").map(_.text.trim)
      dsItem(dkey, skey)
    }.toArray

    aggKeySqlQuerys = xmlNode.map { iatFea =>

      val Array(dkey, skey) = Array(
        iatFea \ "dkey",
        iatFea \ "aggregator" \ "skey"
      ).map(_.text.trim)

      val valkey = (iatFea \ "valkey" \ "key").map(_.text.trim).toList

      ValKeyItems = (iatFea \ "valkey").map { x =>
        val Array(key, truncate, space, iatconf) = Array(
          x \ "key",
          x \ "keytruncate",
          x \ "iatfuncspace",
          x \ "iatconf"
        )

        val IatConfItems = iatconf.map { x =>
          val Array(func, space, truncate, iatfeas) = Array(
            x \ "iatfunc",
            x \ "iatfuncspace",
            x \ "iattruncate",
            x \ "iatfea"
          )

          val feaArr:Array[feaItem] = iatfeas.map { x=>
            val Array(key, space, agg) = Array(
              x \ "iatfkey",
              x \ "iatspace",
              x \ "aggregator"
            )

            val spaceStr = if(space.text.trim == "") "TRUE" else space.text.trim

            feaItem(key.text.trim, Utils.spaceParse(spaceStr), agg)

          }.toArray

          val truncateVal:Float = if(truncate.text.trim != "")  truncate.text.trim.toFloat else -1.0f

          val spaceStr = if(space.text.trim == "") "TRUE" else space.text.trim

          IatConfItem(func.text.trim, Utils.spaceParse(spaceStr), truncateVal, feaArr)

        }.toArray

        val truncateFloat = if(truncate.text.trim !="") truncate.text.trim.toFloat else -1.0f

        val spaceStr = if(space.text.trim == "") "TRUE" else space.text.trim

        ValKeyItem(key.text.trim, truncateFloat, Utils.spaceParse(spaceStr), IatConfItems)

      }.toArray

      ValKeys = ValKeyItems.map{ item =>
        val key:String = item.key
        val truncate:Float = item.truncate
        val space:String = item.space
        if(truncate == -1.0f) {
          s"IF(${space}, ${key}, NULL) AS ${key}"
        } else {
          s"IF(${space}, IF(${key} <= ${truncate}, ${key}, ${truncate}), NULL) AS ${key}"
        }
      }.mkString(",\n")

      iatConfs = ValKeyItems.map { item =>
        val key:String = item.key
        val iatConfArr:Array[IatConfItem] = item.iatConfArr

        val iatTerms = iatConfArr.map{ item =>
          val func = item.func
          val space = item.space
          val truncateVal = item.iattruncate

          if(truncateVal == -1.0f) {
            Utils.iatFuncMatch(func, key, space)
          } else {
            Utils.iatFuncMatch(func, key, space, truncateVal)
          }
        }

        iatTerms
      }.flatMap(x=>x).mkString(",\n")

      feaConfs = ValKeyItems.map{ item =>
        val key:String = item.key
        val iatConfArr:Array[IatConfItem] = item.iatConfArr

        val iatTerms = iatConfArr.map{ item =>
          val func = item.func

          val feaArr = item.feaArr
          val feas = feaArr.map { item =>
            val fkey:String =  item.fkey
            val space:String = item.space
            val aggXml: NodeSeq = item.agg

            val aggKey:String = s"iat_${key}_${func}"

            Utils.feaAggMatch(fkey, aggKey, space, aggXml)
          }
          feas
        }.flatMap(x => x)

        iatTerms
      }.flatMap(x=>x).mkString(",\n")

      fKeys  = (iatFea \ "valkey" \ "iatconf" \ "iatfea" \ "iatfkey")
        .map(_.text.trim)
        .map{ fkey => s"'${fkey}', \n\t\t ${fkey}"}
        .mkString(",\n")

      fieldKeyList = fieldKeyList ++ valkey;

      logInfo = Utils.basicInfo(groupNode)
      logSpace = logInfo.space

      groupName = logInfo.groupName
      dKey = dkey.split(",").map(_.trim).mkString(",")
      logFactory = LogFactory.get(logInfo.baseLog)

      val dKeyToString:String = dkey.split(",").map(_.trim).mkString("_")
      val sKeyToString:String = skey.split(",").map(_.trim).mkString("_")

      aggSortKeySqlQuery(dKeyToString, sKeyToString)

    }.mkString(",\n")

    dimKeyStr = dsKeyArr.map(item => item.dkey).distinct.map { dkey =>
      val dkeys = dkey.split(",").map(_.trim).mkString(",")
      val dkeyStr = dkey.split(",").map(_.trim).mkString("_")
      s"concat_ws('|', ${dkeys}) AS ${dkeyStr}_dval"
    }.mkString(",")

    allFieldKeys = fieldKeyList.toSet.toArray.mkString(",\n")

    allRankIds = dsKeyArr.map { item =>
      val dkeys = item.dkey.split(",").map(_.trim).mkString(",")
      val dkeyStr = item.dkey.split(",").map(_.trim).mkString("_")
      val skeys = item.skey.split(",").map(_.trim).mkString(",")
      val skeyStr = item.skey.split(",").map(_.trim).mkString("_")

      s"row_number() OVER (PARTITION BY ${dkeys} ORDER BY ${skeys}) AS ${dkeyStr}_${skeyStr}_rank_id"
    }.mkString(",\n")

    iatRankSqlStr = iatRankSql()

    mergeQueryStr = dsKeyArr.map { item =>
      val dkeyStr = item.dkey.split(",").map(_.trim).mkString("_")
      val skeyStr = item.skey.split(",").map(_.trim).mkString("_")
      val dsKeyStr = dkeyStr + "_" + skeyStr

      s"""
         |(
         |SELECT  dkey,
         |        dval,
         |        fkey,
         |        fval
         |FROM
         |        ${dsKeyStr}_tab
         |)
         |""".stripMargin
    }.mkString("\n UNION ALL")


  }

  def iatRankSql():String = {
    val query =
      s"""
         |iat_tb AS (
         | SELECT    ${dimKeyStr},
         |           ${allFieldKeys},
         |           ${allRankIds}
         |    FROM   ${logFactory.logName}
         |    WHERE  ${logSpace}
         | )
         |""".stripMargin
    query
  }

  def aggSortKeySqlQuery(dKeyToString:String, sKeyToString:String): String = {
    val dsKeyToString:String = dKeyToString + "_" + sKeyToString
    val query =
      s"""
         |
         |${dsKeyToString}_tab AS (
         |
         |SELECT  '${dKeyToString}' AS dkey,
         |        atb.dval AS dval,
         |        btb.fkey AS fkey,
         |        btb.fval AS fval
         |FROM    (
         |            SELECT  dval,
         |                    ${feaConfs}
         |            FROM    (
         |                        SELECT  a.${dKeyToString}_dval AS dval,
         |                                ${iatConfs},
         |                                a.t1 AS rank_num
         |                        FROM    (
         |                                    SELECT  ${dKeyToString}_dval,
         |                                            ${ValKeys},
         |                                            ${dKeyToString}_${sKeyToString}_rank_id AS t1
         |                                    FROM    iat_tb
         |                                ) a
         |                        INNER JOIN
         |                                (
         |                                    SELECT  ${dKeyToString}_dval,
         |                                            ${ValKeys},
         |                                            ${dKeyToString}_${sKeyToString}_rank_id - 1 AS t2
         |                                    FROM    iat_tb
         |                                ) b
         |                        ON      a.${dKeyToString}_dval = b.${dKeyToString}_dval
         |                        AND     a.t1 = b.t2
         |                    )
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
         | )
         |""".stripMargin

    query

  }

  def sqlQuery(): String = {
    val writeSql:String =
      s"""
         |INSERT OVERWRITE TABLE ${outHiveTable} PARTITION (
         |    date = '${date}',
         |    hour = '${hour}',
         |    aggregator_type = 'iat',
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

    val query :String = iatRankSqlStr + "," + aggKeySqlQuerys + writeSql + "\n" + mergeQuery

    query

  }

  def compute(spark:SparkSession): Unit = {
    val log = logFactory.logString(date, hour, window)
    val query:String = log + ",\n" + sqlQuery()
    println(query)
    spark.sql(query)
  }

}
