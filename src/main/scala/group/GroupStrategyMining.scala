/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame,Column}
import org.apache.spark.sql.functions._
import scala.util.parsing.json._
import scala.xml.NodeSeq

case class GridDimItem(fKey: String, low: Double, high:Double, step:Double) {
  override def toString: String = {
    s"${fKey}-${low}-${high}-${step}"
  }
}

case class StrategyItem(schema: String, threshold: Array[GridDimItem]) {
  override def toString: String = {
    s"${schema}-${threshold.mkString("|")}"
  }
}


class GroupStrategyMining  extends Serializable {
  var confParamMap:Map[String, Map[String, Array[Double]]] = null
  var date:String = ""
  var version:String = ""

  var schemaItems: Array[SchemaItem] = null
  var schemaItemStr:String = ""
  var strategyItems:Array[StrategyItem] = null


  def initialize(groupNode: NodeSeq, args:GroupExecutorArgs):Unit = {

    this.date = args.date
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

          var threshArr = (for(i <- 0 to steps) yield (low+i*step).toDouble).toArray
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

  }

 private def sqlQuery():String = {

    val queryStr =
      s"""
         |WITH group_data AS (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_feature_hourly
         |    WHERE   date = '${date}'
         |    AND     hour = '23'
         |    AND     version = '${version}'
         |    AND     group_schema IN (${schemaItemStr})
         |
         |),
         |game_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS game_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_game_performance_feature_daily
         |    WHERE   date = '${date}'
         |    AND     version = '${version}'
         |    AND     group_schema IN (${schemaItemStr})
         |),
         |ug_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS ug_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_ug_performance_feature_daily
         |    WHERE   date = '${date}'
         |    AND     version = '${version}'
         |    AND     group_schema IN (${schemaItemStr})
         |),
         |ecom_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS ecom_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_ecom_order_performance_feature_daily
         |    WHERE   date = '${date}'
         |    AND     version = '${version}'
         |    AND     group_schema IN (${schemaItemStr})
         |)
         |SELECT  t1.group_schema AS group_schema,
         |        t1.group_pattern AS group_pattern,
         |        t1.feature_json AS feature_json,
         |        t2.game_json AS game_json,
         |        t3.ug_json AS ug_json,
         |        t4.ecom_json AS ecom_json
         |FROM    group_data t1
         |LEFT JOIN
         |        game_performance t2
         |ON      t1.group_schema = t2.group_schema
         |AND     t1.group_pattern = t2.group_pattern
         |LEFT JOIN
         |        ug_performance t3
         |ON      t1.group_schema = t3.group_schema
         |AND     t1.group_pattern = t3.group_pattern
         |LEFT JOIN
         |        ecom_performance t4
         |ON      t1.group_schema = t4.group_schema
         |AND     t1.group_pattern = t4.group_pattern
         |
         |
         |
         |""".stripMargin

    println(queryStr)

    queryStr

  }



  def strategyMining(spark:SparkSession): Unit = {

    val queryStr = sqlQuery()
    val dataDF = spark.sql(queryStr).cache()
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
      val rulesArray = cartesianProd.map(x=>x.mkString("|"))
      rulesArray
     }
    )

    val dataExplodeDF = dataDF
      .withColumn("rulePartition", partitionUDF($"feature_json", $"group_schema"))
      .withColumn("rule_condition", explode($"rulePartition"))

    dataExplodeDF.createOrReplaceTempView("group_rule_ad_performance_table")

    val adPerformanceQuery =
      s"""
         |
         |game_res AS (
         |    SELECT  group_schema,
         |            rule_condition,
         |            SUM(get_json_object(game_json, '$$.game_activation_cnt')) AS game_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_0_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_0_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_0_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_day_0_scale') * get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_0_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_0_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_1_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_1_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_1_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_day_1_scale') * get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_1_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_1_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_2_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_2_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_2_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_day_2_scale') * get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_2_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_2_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_3_10_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_day_3_10_scale') * get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_3_10_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_3_30_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_day_3_30_scale') * get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(game_json, '$$.game_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(game_json, '$$.game_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS game_day_3_30_scale
         |    FROM    group_rule_ad_performance_table
         |    WHERE   game_json IS NOT NULL
         |    AND     game_json != ''
         |    GROUP BY
         |            group_schema,
         |            rule_condition
         |),
         |ug_res (
         |    SELECT  group_schema,
         |            rule_condition,
         |            SUM(get_json_object(ug_json, '$$.ug_activation_cnt')) AS ug_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_0_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_0_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_0_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_day_0_scale') * get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_0_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_0_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_1_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_1_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_1_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_day_1_scale') * get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_1_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_1_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_2_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_2_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_2_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_day_2_scale') * get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_2_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_2_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_3_10_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_day_3_10_scale') * get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_10_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_3_10_scale,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_3_30_activation_cnt,
         |            SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_day_3_30_scale') * get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) / SUM(
         |                IF(
         |                    get_json_object(ug_json, '$$.ug_day_3_30_scale') IS NOT NULL,
         |                    get_json_object(ug_json, '$$.ug_activation_cnt'),
         |                    NULL
         |                )
         |            ) AS ug_day_3_30_scale
         |    FROM    group_rule_ad_performance_table
         |    WHERE   ug_json IS NOT NULL
         |    AND     ug_json != ''
         |    GROUP BY
         |            group_schema,
         |            rule_condition
         |),
         |ecom_res (
         |    SELECT  group_schema,
         |            rule_condition,
         |            SUM(get_json_object(ecom_json, '$$.ord_cnt')) AS ord_cnt,
         |            SUM(get_json_object(ecom_json, '$$.unpay_cnt')) AS unpay_cnt,
         |            SUM(get_json_object(ecom_json, '$$.fail_cnt')) AS fail_cnt,
         |            SUM(get_json_object(ecom_json, '$$.re_cnt')) AS reliable_cnt,
         |            SUM(get_json_object(ecom_json, '$$.unpay_cnt')) / SUM(get_json_object(ecom_json, '$$.ord_cnt')) AS unpay_ratio,
         |            SUM(get_json_object(ecom_json, '$$.fail_cnt')) / SUM(get_json_object(ecom_json, '$$.ord_cnt')) AS fail_ratio,
         |            SUM(get_json_object(ecom_json, '$$.sum_order_amount')) AS sum_order_amount,
         |            SUM(get_json_object(ecom_json, '$$.sum_order_amount')) / SUM(get_json_object(ecom_json, '$$.ord_cnt')) AS order_amount_avg,
         |            SUM(get_json_object(ecom_json, '$$.trust_ord_cnt')) AS trust_ord_cnt,
         |            SUM(get_json_object(ecom_json, '$$.trust_sum_order_amount')) AS trust_sum_order_amount,
         |            SUM(get_json_object(ecom_json, '$$.trust_unpay_cnt')) AS trust_unpay_cnt,
         |            SUM(get_json_object(ecom_json, '$$.trust_fail_cnt')) AS trust_fail_cnt,
         |            SUM(get_json_object(ecom_json, '$$.trust_unpay_cnt')) / SUM(get_json_object(ecom_json, '$$.trust_ord_cnt')) AS trust_unpay_ratio,
         |            SUM(get_json_object(ecom_json, '$$.trust_fail_cnt')) / SUM(get_json_object(ecom_json, '$$.trust_ord_cnt')) AS trust_fail_ratio,
         |            SUM(get_json_object(ecom_json, '$$.trust_sum_order_amount')) / SUM(get_json_object(ecom_json, '$$.trust_ord_cnt')) AS trust_order_amount_avg
         |    FROM    group_rule_ad_performance_table
         |    WHERE   ecom_json IS NOT NULL
         |    AND     ecom_json != ''
         |    GROUP BY
         |            group_schema,
         |            rule_condition
         |)
         |
         |INSERT OVERWRITE TABLE union_anti.ads_union_antispam_group_strategy_mining_daily PARTITION (date = '${date}', version = '${version}')
         |
         |SELECT  COALESCE(T1.group_schema, T2.group_schema, T3.group_schema) AS group_schema,
         |        COALESCE(T1.rule_condition, T2.rule_condition, T3.rule_condition) AS rule_condition,
         |        TO_JSON(
         |            STRUCT(
         |                ord_cnt,
         |                unpay_cnt,
         |                fail_cnt,
         |                reliable_cnt,
         |                unpay_ratio,
         |                fail_ratio,
         |                sum_order_amount,
         |                order_amount_avg,
         |                trust_ord_cnt,
         |                trust_sum_order_amount,
         |                trust_unpay_cnt,
         |                trust_fail_cnt,
         |                trust_unpay_ratio,
         |                trust_fail_ratio,
         |                trust_order_amount_avg,
         |                ug_activation_cnt,
         |                ug_day_0_activation_cnt,
         |                ug_day_0_scale,
         |                ug_day_1_activation_cnt,
         |                ug_day_1_scale,
         |                ug_day_2_activation_cnt,
         |                ug_day_2_scale,
         |                ug_day_3_10_activation_cnt,
         |                ug_day_3_10_scale,
         |                ug_day_3_30_activation_cnt,
         |                ug_day_3_30_scale,
         |                game_activation_cnt,
         |                game_day_0_activation_cnt,
         |                game_day_0_scale,
         |                game_day_1_activation_cnt,
         |                game_day_1_scale,
         |                game_day_2_activation_cnt,
         |                game_day_2_scale,
         |                game_day_3_10_activation_cnt,
         |                game_day_3_10_scale,
         |                game_day_3_30_activation_cnt,
         |                game_day_3_30_scale
         |            )
         |        ) AS ad_performance_json
         |FROM    ecom_res T1
         |FULL OUTER JOIN
         |        ug_res T2
         |ON      T1.group_schema = T2.group_schema
         |AND     T1.rule_condition = T2.rule_condition
         |FULL OUTER JOIN
         |        game_res T3
         |ON      COALESCE(T1.group_schema, T2.group_schema) = T3.group_schema
         |AND     COALESCE(T1.rule_condition, T2.rule_condition) = T3.rule_condition
         |
         |""".stripMargin

    println("==================================")
    println(adPerformanceQuery)
    println("==================================")

    spark.sql(adPerformanceQuery)

  }

}
