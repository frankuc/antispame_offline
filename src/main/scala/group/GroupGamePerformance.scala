package group

import org.apache.spark.sql.SparkSession
import scala.xml.NodeSeq

class GroupGamePerformance extends  Serializable {

  var date:String = ""
  var version:String = ""
  var schemaItems: Array[SchemaItem] = null
  var groupKeyMap:String = ""

  def initialize(groupNode: NodeSeq, args:GroupExecutorArgs) = {
    this.date =  args.date
    this.version = (groupNode \ "group").text.trim

    val schemaNodes = (groupNode \ "schema_list" \ "schema").map(_.text.trim)
    if(schemaNodes.size > 0 ) {
      schemaItems = schemaNodes
        .map{schemaStr => SchemaItem(schemaStr)}
        .toArray
      println(schemaItems.mkString("|"))

      groupKeyMap = schemaItems.map{ schemaItem =>
        val schema = schemaItem.fields
        val lineStr = s"'${schema}'" + ",\n" + s"concat_ws('|', ${schema})"
        lineStr
      }.mkString(",\n")

    }

  }

  def sqlQuery(): String = {

    val groupQuery:String =
      s"""
        |WITH game_performance AS (
        |    SELECT  tb1.ad_package_name AS ad_package_name,
        |            tb1.retention_day AS retention_day,
        |            tb1.stay_time AS stay_time,
        |            tb1.date AS date,
        |            tb1.req_id AS req_id,
        |            tb1.union_baseline_active_ratio AS union_baseline_active_ratio,
        |            tb1.union_baseline_1800_ratio AS union_baseline_1800_ratio,
        |            tb2.*
        |    FROM    (
        |                SELECT  date,
        |                        get_json_object(extra_json, '$$.rit') AS rit,
        |                        get_json_object(extra_json, '$$.ad_package_name') AS ad_package_name,
        |                        get_json_object(extra_json, '$$.union_imei') AS union_imei,
        |                        get_json_object(extra_json, '$$.req_id') AS req_id,
        |                        get_json_object(extra_json, '$$.retention_day') AS retention_day,
        |                        get_json_object(feature_json, '$$.stay_time') AS stay_time,
        |                        get_json_object(feature_json, '$$.union_baseline_active_ratio') AS union_baseline_active_ratio,
        |                        get_json_object(feature_json, '$$.union_baseline_1800_ratio') AS union_baseline_1800_ratio
        |                FROM    dm_ad_antispam.antispam_m_game_activation_sdk_performance_di
        |                WHERE   date = '${date}'
        |            ) tb1
        |    INNER JOIN
        |            (
        |                SELECT  request_id,
        |                        union_api_status,
        |                        status_code,
        |                        rit,
        |                        site_id,
        |                        site_name,
        |                        media_id,
        |                        media_name,
        |                        ad_slot_type,
        |                        device_id,
        |                        user_agent,
        |                        device_type,
        |                        ip,
        |                        latitude,
        |                        longitude,
        |                        version,
        |                        is_paid_app,
        |                        os_type,
        |                        os_version,
        |                        device_vendor,
        |                        device_model,
        |                        model_price,
        |                        connect_type,
        |                        screen_width,
        |                        screen_height,
        |                        ad_show_position,
        |                        log_timestamp,
        |                        mac,
        |                        imei,
        |                        wifi_ssid,
        |                        imsi,
        |                        union_imei,
        |                        oaid,
        |                        sha1,
        |                        idfv,
        |                        global_did,
        |                        applog_did,
        |                        wifi_mac,
        |                        did,
        |                        rom_version,
        |                        ad_sdk_version,
        |                        power_on_timestamp,
        |                        system_compiling_time,
        |                        system_build_version,
        |                        screen_orientation,
        |                        city_id,
        |                        province_id,
        |                        screen_bright,
        |                        is_screen_off,
        |                        battery_remaining_pct,
        |                        cpu_max_freq,
        |                        cpu_min_freq,
        |                        cpu_num,
        |                        is_charging,
        |                        free_space_in,
        |                        sdcard_size,
        |                        is_rooted,
        |                        app_running_time,
        |                        is_low_power,
        |                        boot_timestamp,
        |                        sec_did,
        |                        whale_decision,
        |                        is_reverse,
        |                        is_limit_delivery,
        |                        front_intercept_decision,
        |                        front_intercept_detail,
        |                        mediation_req_type,
        |                        is_refuse,
        |                        waterfall_type,
        |                        is_waterfall_cache,
        |                        is_system_adb,
        |                        is_click_acb,
        |                        last_show_report_detail,
        |                        sdk_plugin_version,
        |                        access_method,
        |                        union_imei_source,
        |                        carrier_id,
        |                        android_id,
        |                        engine_status_code,
        |                        district_id,
        |                        sec_did_2,
        |                        antispam_dispose,
        |                        antispam_list_rules,
        |                        union_imei_first_time_difference,
        |                        p_date,
        |                        hour,
        |                        ad_source,
        |                        final_decision,
        |                        mark_status
        |                FROM    union_anti.dwd_union_anti_request_daily
        |                WHERE   p_date = '${date}'
        |            ) tb2
        |    ON      tb1.req_id = tb2.request_id
        |),
        |reward_app_id AS (
        |    SELECT  app_id
        |    FROM    (
        |                SELECT  site_id AS app_id
        |                FROM
        |                UNION.make_money_online_app
        |                WHERE   date = '${date}'
        |                AND     first_tag_name = '网赚应用'
        |                GROUP BY
        |                        site_id
        |            )
        |    UNION
        |    (
        |        SELECT  app_id
        |        FROM    union_anti.ads_union_antispam_disposal_center_app_status_hourly
        |        WHERE   p_date = '${date}'
        |        AND     disposal_tag IN(12, 42)
        |        AND     is_effective = 'true'
        |        GROUP BY
        |                app_id
        |    )
        |),
        |risk_app_package AS (
        |    SELECT  t2.package_name AS package_name
        |    FROM    (
        |                SELECT  app_id
        |                FROM    reward_app_id
        |                GROUP BY
        |                        app_id
        |            ) t1
        |    JOIN    (
        |                SELECT  site_id,
        |                        package_name
        |                FROM    ad_dim.dim_union_site
        |                WHERE   p_date = '${date}'
        |                GROUP BY
        |                        site_id,
        |                        package_name
        |            ) t2
        |    ON      CAST(t1.app_id AS BIGINT) = t2.site_id
        |    GROUP BY
        |            t2.package_name
        |),
        |group_game_performance AS (
        |    SELECT  group_schema,
        |            group_pattern,
        |            count(DISTINCT union_imei) AS game_uimei_cnt,
        |            SUM(activation_cnt) AS game_activation_cnt,
        |            SUM(st_1800_expect_0) / SUM(st_1800_activation_0) AS game_day_0_scale,
        |            SUM(st_1800_expect_1) / SUM(st_1800_activation_1) AS game_day_1_scale,
        |            SUM(st_1800_expect_2) / SUM(st_1800_activation_2) AS game_day_2_scale,
        |            SUM(st_1800_expect_10) / SUM(st_1800_activation_10) AS game_day_1_10_scale,
        |            SUM(st_1800_expect_3_10) / SUM(st_1800_activation_3_10) AS game_day_3_10_scale,
        |            SUM(st_1800_expect_3_30) / SUM(st_1800_activation_3_30) AS game_day_3_30_scale,
        |            SUM(st_1800_expect_30) / SUM(st_1800_activation_30) AS game_day_1_30_scale
        |    FROM    (
        |                SELECT  t1.union_imei AS union_imei,
        |                        t1.group_schema AS group_schema,
        |                        t1.group_pattern AS group_pattern,
        |                        SUM(
        |                            IF(
        |                                retention_day = 0
        |                                AND union_baseline_active_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS activation_cnt,
        |                        SUM(
        |                            IF(
        |                                retention_day = 0
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_0,
        |                        SUM(
        |                            IF(
        |                                retention_day = 1
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_1,
        |                        SUM(
        |                            IF(
        |                                retention_day = 2
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_2,
        |                        SUM(
        |                            IF(
        |                                retention_day > 0
        |                                AND retention_day <= 10
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_10,
        |                        SUM(
        |                            IF(
        |                                retention_day > 2
        |                                AND retention_day <= 10
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_3_10,
        |                        SUM(
        |                            IF(
        |                                retention_day > 2
        |                                AND retention_day <= 30
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_3_30,
        |                        SUM(
        |                            IF(
        |                                retention_day > 0
        |                                AND stay_time > 1800,
        |                                1 / union_baseline_1800_ratio,
        |                                0
        |                            )
        |                        ) AS st_1800_expect_30,
        |                        SUM(
        |                            IF(
        |                                retention_day = 0
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_0,
        |                        SUM(
        |                            IF(
        |                                retention_day = 1
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_1,
        |                        SUM(
        |                            IF(
        |                                retention_day = 2
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_2,
        |                        SUM(
        |                            IF(
        |                                retention_day > 0
        |                                AND retention_day <= 10
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_10,
        |                        SUM(
        |                            IF(
        |                                retention_day > 2
        |                                AND retention_day <= 10
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_3_10,
        |                        SUM(
        |                            IF(
        |                                retention_day > 2
        |                                AND retention_day <= 30
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_3_30,
        |                        SUM(
        |                            IF(
        |                                retention_day > 0
        |                                AND union_baseline_1800_ratio IS NOT NULL,
        |                                1,
        |                                0
        |                            )
        |                        ) AS st_1800_activation_30
        |                FROM    (
        |                            SELECT  date,
        |                                    rit,
        |                                    ad_package_name,
        |                                    union_imei,
        |                                    req_id,
        |                                    retention_day,
        |                                    stay_time,
        |                                    union_baseline_active_ratio,
        |                                    union_baseline_1800_ratio,
        |                                    group_schema,
        |                                    group_pattern
        |                            FROM    game_performance
        |                            LATERAL VIEW
        |                                    EXPLODE(
        |                                        MAP(
        |                                            ${groupKeyMap}
        |                                        )
        |                                    ) t AS group_schema,
        |                                    group_pattern
        |                        ) t1
        |                LEFT JOIN
        |                        (
        |                            SELECT  package_name
        |                            FROM    risk_app_package
        |                            GROUP BY
        |                                    package_name
        |                        ) t3
        |                ON      t1.ad_package_name = t3.package_name
        |                WHERE   t3.package_name IS NULL
        |                GROUP BY
        |                        t1.union_imei,
        |                        t1.group_schema,
        |                        t1.group_pattern
        |            )
        |    GROUP BY
        |            group_schema,
        |            group_pattern
        |)
        |INSERT OVERWRITE TABLE union_anti.ads_union_antispam_group_aggregator_ad_game_performance_feature_daily PARTITION (date = '${date}', version = '${version}')
        |SELECT  group_schema,
        |        group_pattern,
        |        TO_JSON(
        |            STRUCT(
        |                game_uimei_cnt,
        |                game_activation_cnt,
        |                game_day_0_scale,
        |                game_day_1_scale,
        |                game_day_2_scale,
        |                game_day_1_10_scale,
        |                game_day_3_10_scale,
        |                game_day_3_30_scale,
        |                game_day_1_30_scale
        |            )
        |        ) AS feature_json
        |FROM    group_game_performance
        |
        |
        |""".stripMargin

    groupQuery
  }


  def compute(spark:SparkSession): Unit = {

    val query:String = sqlQuery()

    println(query)

    spark.sql(query)
  }

}
