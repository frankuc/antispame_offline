/**

 feature-group
 shuai zhang
 BeiJing, may 16, 2023
 */

package group

import java.util
import java.util.concurrent.{Callable, Executors, Future}
import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import scala.util.parsing.json._


class GroupStrategyMining  extends Serializable {


  def initialize(groupNode: NodeSeq, args:GroupExecutorArgs):Unit = {

  }

  def loadGroupData(spark:SparkSession) = {

    val queryStr =
      s"""
         |WITH group_data AS (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_feature_hourly
         |    WHERE   date = '20230501'
         |    AND     hour = '23'
         |    AND     version = 'REQ_GROUP_DEVICE_RESOURCE'
         |    AND     group_schema IN ('site_id,city_id,device_model', 'site_id,city_id,district_id,rom_version')
         |
         |),
         |game_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS game_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_game_performance_feature_daily
         |    WHERE   date = '20230501'
         |    AND     version = 'REQ_GROUP_DEVICE_RESOURCE'
         |    AND     group_schema IN ('site_id,city_id,device_model', 'site_id,city_id,district_id,rom_version')
         |),
         |ug_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS ug_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_ug_performance_feature_daily
         |    WHERE   date = '20230501'
         |    AND     version = 'REQ_GROUP_DEVICE_RESOURCE'
         |    AND     group_schema IN ('site_id,city_id,device_model', 'site_id,city_id,district_id,rom_version')
         |),
         |ecom_performance (
         |    SELECT  group_schema,
         |            group_pattern,
         |            feature_json AS ecom_json
         |    FROM    union_anti.ads_union_antispam_group_aggregator_ad_ecom_order_performance_feature_daily
         |    WHERE   date = '20230501'
         |    AND     version = 'REQ_GROUP_DEVICE_RESOURCE'
         |    AND     group_schema IN ('site_id,city_id,device_model', 'site_id,city_id,district_id,rom_version')
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

    val dataDF = spark.sql(queryStr)

    dataDF
  }


  def strategyMining(dataDF:DataFrame): Unit = {

    val partitionUDF = udf((featureJson:String, paramMap:Map[String, Array[Double]]) => {
      val jsonValueMap = JSON.parseFull(featureJson).get.asInstanceOf[Map[String,Double]]
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


  }

}
