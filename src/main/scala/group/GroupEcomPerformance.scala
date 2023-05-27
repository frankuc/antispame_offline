package group

class GroupEcomPerformance extends  Serializable {

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
        |WITH ec_order_performance AS (
        |    SELECT  tb1.create_order_amount AS create_order_amount,
        |            tb1.is_pay AS is_pay,
        |            tb1.is_fail AS is_fail,
        |            tb1.is_reliable AS is_reliable,
        |            tb1.is_refunded AS is_refunded,
        |            tb1.is_cancel AS is_cancel,
        |            tb1.order_id AS order_id,
        |            tb1.id AS uimei,
        |            tb2.*
        |    FROM    (
        |                SELECT  get_json_object(feature_json, '$$.create_order_amount') AS create_order_amount,
        |                        get_json_object(feature_json, '$$.is_pay') AS is_pay,
        |                        get_json_object(feature_json, '$$.is_fail') AS is_fail,
        |                        get_json_object(feature_json, '$$.is_reliable') AS is_reliable,
        |                        get_json_object(extra_json, '$$.rit') AS rit,
        |                        get_json_object(extra_json, '$$.req_id') AS request_id,
        |                        get_json_object(feature_json, '$$.is_refunded') AS is_refunded,
        |                        get_json_object(feature_json, '$$.is_cancel') AS is_cancel,
        |                        get_json_object(extra_json, '$$.order_id') AS order_id,
        |                        get_json_object(extra_json, '$$.union_imei') AS id,
        |                        p_date
        |                FROM    union_anti.ads_ad_object_order_performance_daily
        |                WHERE   p_date = '${date}'
        |                AND     CAST(get_json_object(extra_json, '$$.rit') AS BIGINT) >= 800000000
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
        |    ON      tb1.request_id = tb2.request_id
        |),
        |group_ec_order_performance AS (
        |    SELECT  tb1.group_schema AS group_schema,
        |            tb1.group_pattern AS group_pattern,
        |            SUM(1) AS ord_cnt,
        |            SUM(1 - is_pay) AS unpay_cnt,
        |            SUM(is_fail) AS fail_cnt,
        |            SUM(1 - is_pay) / SUM(1) AS unpay_ratio,
        |            SUM(is_fail) / SUM(1) AS fail_ratio,
        |            SUM(is_reliable) AS re_cnt,
        |            SUM(create_order_amount) AS sum_order_amount,
        |            COUNT(IF(create_order_amount > 20, order_id, NULL)) AS trust_ord_cnt,
        |            SUM(IF(create_order_amount > 20, create_order_amount, 0)) AS trust_sum_order_amount,
        |            SUM(IF(create_order_amount > 20, 1 - is_pay, 0)) AS trust_unpay_cnt,
        |            SUM(IF(create_order_amount > 20, is_fail, 0)) AS trust_fail_cnt,
        |            SUM(IF(create_order_amount > 20, 1 - is_pay, 0)) / COUNT(IF(create_order_amount > 20, order_id, NULL)) AS trust_unpay_ratio,
        |            SUM(IF(create_order_amount > 20, is_fail, 0)) / COUNT(IF(create_order_amount > 20, order_id, NULL)) AS trust_fail_ratio
        |    FROM    (
        |                SELECT  create_order_amount,
        |                        is_pay,
        |                        is_fail,
        |                        is_reliable,
        |                        rit,
        |                        request_id,
        |                        is_refunded,
        |                        is_cancel,
        |                        order_id,
        |                        uimei,
        |                        p_date,
        |                        group_schema,
        |                        group_pattern
        |                FROM    ec_order_performance
        |                LATERAL VIEW
        |                        EXPLODE(
        |                            MAP(
        |                               ${groupKeyMap}
        |                            )
        |                        ) t AS group_schema,
        |                        group_pattern
        |            ) tb1
        |    GROUP BY
        |            tb1.group_schema,
        |            tb1.group_pattern
        |)
        |INSERT OVERWRITE TABLE union_anti.ads_union_antispam_group_aggregator_ad_ecom_order_performance_feature_daily PARTITION (date = '${date}', version = '${version}')
        |SELECT  group_schema,
        |        group_pattern,
        |        TO_JSON(
        |            STRUCT(
        |                ord_cnt,
        |                unpay_cnt,
        |                fail_cnt,
        |                unpay_ratio,
        |                fail_ratio,
        |                re_cnt,
        |                sum_order_amount,
        |                trust_ord_cnt,
        |                trust_sum_order_amount,
        |                trust_unpay_cnt,
        |                trust_fail_cnt,
        |                trust_unpay_ratio,
        |                trust_fail_ratio
        |            )
        |        ) AS feature_json
        |FROM    group_ec_order_performance
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
