/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import feature.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import scala.util.parsing.json._
import scala.xml.NodeSeq
import feature.Utils.spaceParse

case class optTargetItem(optSpace:String) {
  override def toString:String = {
    s"${optSpace}"
  }
}


class GroupPatternMatch extends Serializable {
  var confParamMap:Map[String, Map[String, Array[Double]]] = null
  var date:String = ""
  var version:String = ""
  var strategyDate:String = ""

  var schemaItems: Array[SchemaItem] = null
  var schemaItemStr:String = ""
  var strategyItems:Array[StrategyItem] = null
  var optTargetStr:String = ""
  var optTargetItems:Array[optTargetItem] = null


  def initialize(groupNode: NodeSeq, args:GroupPatternMatchArgs):Unit = {

    this.date = args.date
    this.strategyDate = args.strategyDate
    this.version = (groupNode \ "group").text.trim

    val strategyNodes = (groupNode \ "strategy_list" \ "strategy")
    if(strategyNodes.size > 0) {
      strategyItems = strategyNodes.map{ x =>
        var Array(schema) = Array(x \ "schema").map(_.text.trim)
        val gridDimItems = (x \ "threshold" \ "grid-dim").map { y =>
          var Array(fKey, low, high, step) = Array(y \ "feature", y \ "low", y \ "high", y \ "step").map(_.text.trim)
          GridDimItem(fKey, low.toDouble, high.toDouble, step.toDouble)
        }.toArray

        StrategyItem(schema, gridDimItems)
      }.toArray

      schemaItems = strategyItems.map{x => SchemaItem(x.schema)}
      schemaItemStr = schemaItems.map{ schemaItem =>
        val schema = schemaItem.fields
        val lineStr = s"'${schema}'"
        lineStr
      }.mkString(",")

      confParamMap = strategyItems.map{ x =>
        val schemaStr = x.schema

        val thresholdMap = x.threshold.map { item:GridDimItem =>
          val fKey: String = item.fKey
          val low: Double = item.low
          val high:Double = item.high
          val step:Double = item.step
          val steps = scala.math.floor(((high - low) / step)).toInt

          var threshArr = (for(i <- 0 to steps) yield (low+i*step).toDouble).toArray.map{ x =>
            x.formatted("%.3f").toDouble }

          if(threshArr.max < high) threshArr = threshArr :+ high
          (fKey, threshArr)
        }.toMap

        (schemaStr, thresholdMap)
      }.toMap

      confParamMap.map{case (k,v) =>
        println(k)
        v.map{case(a,b) =>
          println(a)
          println(b.mkString("_"))
        }
      }

    }

    val optNodes = (groupNode \ "optimization" \ "opt-target")
    if(optNodes.size > 0) {
      optTargetItems = optNodes.map{ x =>
        optTargetItem(Utils.spaceParse(x.text.trim))
      }.toArray
      optTargetStr = optTargetItems.map{ item =>
        item.optSpace
      }.mkString("\nAND\t\t")
    }

  }



  def groupPatternMatch(spark:SparkSession): Unit = {
    import spark.implicits._

    val patternQuery =
      s"""
         |SELECT  group_schema,
         |        group_pattern,
         |        feature_json
         |FROM    union_anti.ads_union_antispam_group_aggregator_feature_hourly
         |WHERE   date = '${date}'
         |AND     hour = '23'
         |AND     version = '${version}'
         |AND     group_schema IN (${schemaItemStr})
         |
         |""".stripMargin

    println(patternQuery)

    val patternDF = spark.sql(patternQuery)

    val thresholdMap:Map[String, Map[String, Array[Double]]] = confParamMap

    val partitionUDF = udf((featureJson:String, group_schema:String) => {
      val jsonValueMap = JSON.parseFull(featureJson).get.asInstanceOf[Map[String,Double]]
      val paramMap = thresholdMap.getOrElse(group_schema, Map[String, Array[Double]]())

      val partitionMap = paramMap.map { case(feature, valueArr) =>
        val fVal:Double = jsonValueMap.getOrElse(feature, -1.0D)
        var partArr:Array[Double] = null
        val maxVal = valueArr.max
        val minVal = valueArr.min

        if(fVal < minVal) {
          partArr = Array(-1.0D)
        } else if(fVal >= maxVal) {
          partArr = valueArr
        } else {
          val UpIndex = valueArr.zipWithIndex.map{case (v, i) => if(v > fVal) i else -1}.filter(x => x > -1).min
          partArr = valueArr.slice(0, UpIndex).toArray
        }
        feature -> partArr
      }

      val featureValuePairs = partitionMap.map { case (k,v) => v.map(i => k -> i).toList }
      val headPairs = featureValuePairs.head.toArray.map(x => Array(x))

      val cartesianProd = featureValuePairs.tail.toArray.foldLeft(headPairs)( (acc, elem) =>
        for (x <- acc; y <- elem )  yield x :+ y
      )
      val rulesArray = cartesianProd.map { x =>
        x.map { t => List(t._1, t._2).map(_.toString).mkString(" >= ") }.mkString(" and ")
      }
      rulesArray
    }
    )

    val patternExplodeDF = patternDF
      .withColumn("rulePartition", partitionUDF($"feature_json", $"group_schema"))
      .withColumn("rule_condition", explode($"rulePartition"))

    patternExplodeDF.createOrReplaceTempView("group_pattern_partition_table")

    val blackPatternQuery =
      s"""
         |
         |WITH strategyTB AS (
         |    SELECT  group_schema,
         |            rule_condition
         |    FROM    (
         |                SELECT  group_schema,
         |                        rule_condition,
         |                        get_json_object(ad_performance_json, '$$.ug_activation_cnt') AS ug_activation_cnt,
         |                        get_json_object(ad_performance_json, '$$.ug_day_3_10_scale') AS ug_day_3_10_scale,
         |                        get_json_object(ad_performance_json, '$$.game_activation_cnt') AS game_activation_cnt,
         |                        get_json_object(ad_performance_json, '$$.game_day_3_10_scale') AS game_day_3_10_scale,
         |                        get_json_object(ad_performance_json, '$$.ord_cnt') AS ord_cnt,
         |                        get_json_object(ad_performance_json, '$$.trust_fail_ratio') AS trust_fail_ratio
         |                FROM    union_anti.ads_union_antispam_group_strategy_mining_daily
         |                WHERE   date = '${strategyDate}'
         |                AND     version = '${version}'
         |            )
         |    WHERE  ${optTargetStr}
         |),
         |
         |patternTB AS (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json,
         |            rule_condition
         |    FROM    group_pattern_partition_table
         |)
         |
         |INSERT OVERWRITE TABLE union_anti.ads_union_antispam_group_strategy_mining_anomaly_pattern_daily PARTITION (date = '${date}', version = '${version}')
         |SELECT  patternTB.group_schema AS group_schema,
         |        patternTB.group_pattern AS group_pattern,
         |        patternTB.feature_json AS feature_json
         |FROM    strategyTB
         |INNER JOIN
         |        patternTB
         |ON      strategyTB.group_schema = patternTB.group_schema
         |AND     strategyTB.rule_condition = patternTB.rule_condition
         |GROUP BY
         |        patternTB.group_schema,
         |        patternTB.group_pattern,
         |        patternTB.feature_json
         |
         |""".stripMargin

    println(blackPatternQuery)

    spark.sql(blackPatternQuery)

  }

}
