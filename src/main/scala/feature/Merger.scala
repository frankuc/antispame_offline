package feature

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq

case class FeatureItem(fKey: String, nvf: Float,
                       truncate: Float, func: String) {
  override def toString: String = {
    s"${fKey}-${nvf.toString}-${truncate.toString}-${func}"
  }
}


class Merger {
  var date:String = ""
  var hour:String = ""
  var group:String = ""
  var version:String = ""
  var dKey:String = ""
  var outputHdfs:String = ""
  var outPutHiveTable:String = ""
  var inputHiveTable:String = ""
  var aggTypes:String = ""
  var feaItems:Array[FeatureItem] = null
  var feaConfStr:String = null
  var aggTypeStr:String = ""

  var populationStats:Boolean = false
  var fKeys:String = ""
  var avgConfStr : String = ""
  var stdConfStr : String = ""


  def initialize(groupNode: NodeSeq, args:MergeExecutorArgs): Unit = {
    var Array(inputTable, group, date,
        hour, dKey, aggType, version, population) = Array(
      groupNode \ "input-tab",
      groupNode \ "group",
      groupNode \ "date",
      groupNode \ "hour",
      groupNode \ "dkey",
      groupNode \ "aggtype",
      groupNode \ "version",
      groupNode \ "population-stats"
    ).map(_.text.trim)

    if(args.date != "") {
      date = args.date
    }

    if(args.hour != "") {
      hour = args.hour
    }

    if(population != "") {
      populationStats = population.toBoolean
    }

    this.date = date
    this.hour = hour
    this.inputHiveTable = inputTable
    this.group = group
    this.dKey = dKey
    this.aggTypes = aggType
    this.version = version

    val Array(outTab, hdfsPath) = Array(
      groupNode \ "output" \ "tab",
      groupNode \ "output" \ "hdfs"
    ).map(_.text.trim)

    this.outPutHiveTable = outTab
    this.outputHdfs = hdfsPath

    feaItems = (groupNode \ "feature").map { feaNode =>
      val Array(fkey, nvf, truncate, func) = Array(
        feaNode  \ "fkey",
        feaNode  \ "nvf",
        feaNode \ "truncate",
        feaNode  \ "func"
      ).map(_.text.trim)

      FeatureItem(
        fkey,
        if(nvf == "") -1.0F else nvf.toFloat,
        if(truncate == "") -1.0F else truncate.toFloat,
        func
      )
    }.toArray

    fKeys = feaItems.map { item =>
      val fkey: String = item.fKey
      s"'${fkey}', \n\t\t ${fkey}"
    }.mkString(",\n\t\t")

    avgConfStr = feaItems.map { item =>
      val fkey: String = item.fKey
      val truncate: Float =  item.truncate
      val func: String = item.func
      var express = ""

      if(truncate == -1.0F) {
        express =  func match {
          case "" => s"AVG(IF(fkey = '${fkey}', fval, NULL)) AS ${fkey}"
          case _  => s"AVG(IF(fkey = '${fkey}', ${func}(fval), NULL)) AS ${fkey}"
        }
      } else {
        express =  func match {
          case "" => s"AVG(IF(fkey = '${fkey}', IF(fval >= ${truncate}, ${truncate}, fval), NULL)) AS ${fkey}"
          case _ =>  s"AVG(IF(fkey = '${fkey}', IF(fval >= ${truncate}, ${func}(${truncate}), ${func}(fval)), NULL)) AS ${fkey}"
        }
      }

      express
    }.mkString(",\n\t\t\t\t")


    stdConfStr = feaItems.map { item =>
      val fkey: String = item.fKey
      val truncate: Float =  item.truncate
      val func: String = item.func
      var express = ""

      if(truncate == -1.0F) {
        express =  func match {
          case "" => s"STDDEV_POP(IF(fkey = '${fkey}', fval, NULL)) AS ${fkey}"
          case _  => s"STDDEV_POP(IF(fkey = '${fkey}', ${func}(fval), NULL)) AS ${fkey}"
        }
      } else {
        express =  func match {
          case "" => s"STDDEV_POP(IF(fkey = '${fkey}', IF(fval >= ${truncate}, ${truncate}, fval), NULL)) AS ${fkey}"
          case _ =>  s"STDDEV_POP(IF(fkey = '${fkey}', IF(fval >= ${truncate}, ${func}(${truncate}), ${func}(fval)), NULL)) AS ${fkey}"
        }
      }

      express
    }.mkString(",\n\t\t\t\t")


    feaConfStr = feaItems.map { item =>
      val fKey: String = item.fKey
      val nvf: Float = item.nvf
      val truncate: Float =  item.truncate
      val func: String = item.func
      var express = ""
      if(truncate == -1.0F) {
        express =  func match {
          case "" => s"NVL( MAX( CASE WHEN fkey = '${fKey}' THEN round(fval, 4) ELSE NULL END ), ${nvf} ) AS ${fKey}"
          case _ => s"NVL( MAX( CASE WHEN fkey = '${fKey}' THEN round(${func}(fval), 4) ELSE NULL END ), ${nvf} ) AS ${fKey}"
        }
      } else {
        express =  func match {
          case "" =>
            s""" NVL( MAX(CASE WHEN fkey = '${fKey}' THEN
               | IF(fval >= ${truncate}, ${truncate}, round(fval, 4)) ELSE NULL END), ${nvf} ) AS ${fKey}
               | """.stripMargin

          case _ =>  s""" NVL( MAX(CASE WHEN fkey = '${fKey}' THEN
                        |IF(fval >= ${truncate}, ${func}(${truncate}), round(${func}(fval), 4) ) ELSE NULL END),
                        | ${nvf} ) AS ${fKey}
                        | """.stripMargin
        }

      }

      express

    }.mkString(",\n\t\t\t\t")

    aggTypeStr = aggTypes
      .split(",")
      .map(x => s"'${x}'").mkString(",")

  }

  def writeHive():String = {
    val query:String =
      s"""
         |INSERT OVERWRITE TABLE ${outPutHiveTable} PARTITION (date = '${date}', hour = '${hour}')
         |""".stripMargin
    query
  }

  def writeHdfs():String = {
    val query:String =
      s"""
         |INSERT OVERWRITE directory '${outputHdfs}/feature/${group}/${version}/${date}/${hour}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |""".stripMargin
    query
  }

  def mergeSql():String = {
    val query:String =
      s"""
         |SELECT
         |        dval AS ${dKey},
         |        ${feaConfStr}
         |FROM    ${inputHiveTable}
         |WHERE   date = '${date}'
         |AND     hour = '${hour}'
         |AND     dkey = '${dKey}'
         |AND     aggregator_type IN (${aggTypeStr})
         |AND     version = '${version}'
         |GROUP BY
         |        dval
         |
         |""".stripMargin
    query
  }

  def population(spark:SparkSession): Unit = {
    val writeQuery:String =
      s"""
         |INSERT OVERWRITE directory '${outputHdfs}/population_stats/${group}/${version}/${date}/${hour}' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         |""".stripMargin

    val query:String =
      s"""
         |SELECT  fkey,
         |        MAX(CASE WHEN stat = 'avg' THEN round(fval, 4) ELSE NULL END) AS avg_value,
         |        MAX(CASE WHEN stat = 'stddev' THEN round(fval, 4) ELSE NULL END) AS std_value
         |FROM    (
         |            (
         |                SELECT  'avg' AS stat,
         |                        btb.fkey AS fkey,
         |                        btb.fval AS fval
         |                FROM    (
         |                            SELECT  ${avgConfStr}
         |                            FROM    ${inputHiveTable}
         |                            WHERE   date = '${date}'
         |                            AND     hour = '${hour}'
         |                            AND     dkey = '${dKey}'
         |                            AND     aggregator_type IN (${aggTypeStr})
         |                            AND     version = '${version}'
         |                        ) atb
         |                LATERAL VIEW
         |                        EXPLODE (
         |                            MAP(
         |                                ${fKeys}
         |                            )
         |                        ) btb AS fkey,
         |                        fval
         |            )
         |            UNION ALL
         |            (
         |                SELECT 'stddev' AS stat,
         |                        btb.fkey AS fkey,
         |                        btb.fval AS fval
         |                FROM    (
         |                            SELECT  ${stdConfStr}
         |                            FROM    ${inputHiveTable}
         |                            WHERE   date = '${date}'
         |                            AND     hour = '${hour}'
         |                            AND     dkey = '${dKey}'
         |                            AND     aggregator_type IN (${aggTypeStr})
         |                            AND     version = '${version}'
         |                        ) atb
         |                LATERAL VIEW
         |                        EXPLODE (
         |                            MAP(
         |                                ${fKeys}
         |                            )
         |                        ) btb AS fkey,
         |                        fval
         |            )
         |        )
         |GROUP BY
         |        fkey
         |
         |""".stripMargin

    val exeSQL = writeQuery + "\n" + query
    println(exeSQL)
    spark.sql(exeSQL)
  }

  def runMerge(spark:SparkSession):Unit = {
    if(populationStats) {
      population(spark)
    }

    if(this.outPutHiveTable != "") {
      val query :String = writeHive() + "\n" + mergeSql()
      println(query)
      spark.sql(query)
    }

    if(this.outputHdfs != "") {
      val query :String = writeHdfs() + "\n" + mergeSql()
      println(query)
      spark.sql(query)
    }

  }

}
