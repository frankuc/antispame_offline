package log

class UnionReqLog extends Log {
  var logName: String = "UnionReqLog"

  def logString(date:String, hour:String, windowSize:Int): String ={
  val sqlQuery:String =
    s"""
      |WITH ${logName} AS (
      |SELECT  request_id AS req_id,
      |        union_api_status,
      |        status_code,
      |        rit,
      |        site_id,
      |        site_name,
      |        media_id,
      |        media_name,
      |        ad_slot_type,
      |        device_id,
      |        user_agent,
      |        device_type,
      |        ip,
      |        latitude,
      |        longitude,
      |        version,
      |        is_paid_app,
      |        os_type,
      |        os_version,
      |        device_vendor,
      |        device_model,
      |        model_price,
      |        connect_type,
      |        screen_width,
      |        screen_height,
      |        ad_show_position,
      |        log_timestamp,
      |        mac,
      |        imei,
      |        wifi_ssid,
      |        imsi,
      |        union_imei,
      |        oaid,
      |        sha1,
      |        idfv,
      |        global_did,
      |        applog_did,
      |        wifi_mac,
      |        did,
      |        rom_version,
      |        ad_sdk_version,
      |        power_on_timestamp,
      |        system_compiling_time,
      |        system_build_version,
      |        screen_orientation,
      |        city_id,
      |        province_id,
      |        screen_bright,
      |        is_screen_off,
      |        battery_remaining_pct,
      |        cpu_max_freq,
      |        cpu_min_freq,
      |        cpu_num,
      |        is_charging,
      |        free_space_in,
      |        sdcard_size,
      |        is_rooted,
      |        app_running_time,
      |        is_low_power,
      |        boot_timestamp,
      |        sec_did,
      |        whale_decision,
      |        is_reverse,
      |        is_limit_delivery,
      |        front_intercept_decision,
      |        front_intercept_detail,
      |        mediation_req_type,
      |        is_refuse,
      |        waterfall_type,
      |        is_waterfall_cache,
      |        is_system_adb,
      |        is_click_acb,
      |        last_show_report_detail,
      |        sdk_plugin_version,
      |        access_method,
      |        union_imei_source,
      |        carrier_id,
      |        android_id,
      |        engine_status_code,
      |        basy_antispam_features,
      |        district_id,
      |        origin_front_intercept_decision,
      |        sec_did_2,
      |        antispam_dispose,
      |        antispam_list_rules,
      |        antispam_mark_rules,
      |        union_imei_first_time_difference,
      |        p_date,
      |        hour,
      |        ad_source,
      |        final_decision,
      |        mark_status
      |FROM    union_anti.dwd_union_anti_request_daily
      |WHERE   p_date >= FROM_UNIXTIME(UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize - 1}, 'yyyyMMdd')
      |AND     p_date <= '${date}'
      |AND     concat(p_date, ' ', hour) >= FROM_UNIXTIME(UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize - 1}, 'yyyyMMdd HH')
      |AND     concat(p_date, ' ', hour) <= concat('${date}', ' ', '${hour}')
      |
      |)
      |""".stripMargin

    sqlQuery
  }
}
