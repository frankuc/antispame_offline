package log

class UnionConvertWideLog extends Log {
  var logName: String = "UnionConvertWideLog"

  def logString(date:String, hour:String, windowSize:Int): String ={
  val sqlQuery:String =
    s"""
      |WITH ${logName} AS (
      |SELECT  req_id,
      |        label,
      |        union_imei,
      |        rit,
      |        site_id,
      |        site_name,
      |        media_id,
      |        media_company_name,
      |        os_version,
      |        model,
      |        device_vendor,
      |        ip,
      |        city_id,
      |        city_name,
      |        province_id,
      |        province_name,
      |        sys_compiling_time,
      |        ad_slot_type,
      |        connect_type,
      |        mac,
      |        imei,
      |        wifi_ssid,
      |        wifi_mac,
      |        imsi,
      |        oaid,
      |        idfv,
      |        global_did,
      |        rom_version,
      |        boot_timestamp,
      |        system_build_version,
      |        battery_remaining_pct,
      |        cpu_num,
      |        is_charging,
      |        free_space_in,
      |        sdcard_size,
      |        is_rooted,
      |        sec_did,
      |        is_system_adb,
      |        is_click_acb,
      |        sdk_plugin_version,
      |        access_method,
      |        carrier_id,
      |        did,
      |        ad_sdk_version,
      |        app_name,
      |        app_id,
      |        app_package_name,
      |        advertiser_id,
      |        advertiser_name,
      |        customer_id,
      |        customer_name,
      |        screen_height,
      |        screen_width,
      |        district_id,
      |        district_name,
      |        hour,
      |        advertiser_first_industry_id,
      |        advertiser_second_industry_id,
      |        advertiser_first_industry_name,
      |        advertiser_second_industry_name,
      |        sec_did_2,
      |        external_actions,
      |        deep_external_action,
      |        event_type,
      |        invalid_event,
      |        is_old_phone,
      |        phone_launch_date,
      |        callback_param,
      |        fraud_type,
      |        creative_id,
      |        ad_id,
      |        is_new_device,
      |        cpa_bid,
      |        pay_amount,
      |        union_imei_first_time_difference,
      |        rit_name,
      |        uid,
      |        date,
      |        classify,
      |        landing_type,
      |        os,
      |        ad_source
      |FROM    dm_adunion.union_convert_device_wide_info_daily
      |WHERE   date >= FROM_UNIXTIME(
      |            UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize - 1},
      |            'yyyyMMdd'
      |        )
      |AND     date <= '${date}'
      |AND     concat(date, ' ', hour) >= FROM_UNIXTIME(
      |            UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize - 1},
      |            'yyyyMMdd HH'
      |        )
      |AND     concat(date, ' ', hour) <= concat('${date}', ' ', '${hour}')
      |
      |)
      |""".stripMargin

    sqlQuery
  }
}
