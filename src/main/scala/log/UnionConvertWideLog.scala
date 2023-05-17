package log


class EcommConvertLog extends Log {
  var logName: String = "EcommConvertLog"

  def logString(date:String, hour:String, windowSize:Int): String ={
  val sqlQuery:String =
    s"""
      |WITH reward_app_id AS (
      |    SELECT  app_id,
      |            MIN(app_type) AS app_type
      |    FROM    (
      |                (
      |                    SELECT  site_id AS app_id,
      |                            99999 AS app_type
      |                    FROM
      |                    UNION.make_money_online_app
      |                    WHERE   date <= '${date}'
      |                    AND     date >= '${date}'
      |                    AND     first_tag_name = '网赚应用'
      |                    GROUP BY
      |                            site_id
      |                )
      |                UNION
      |                (
      |                    SELECT  app_id,
      |                            disposal_tag AS app_type
      |                    FROM    union_anti.ads_union_antispam_disposal_center_app_status_hourly
      |                    WHERE   p_date = '${date}'
      |                    AND     hour = '${hour}'
      |                    AND     disposal_tag IN(12, 42, 10003, 10004, 10005)
      |                    AND     is_effective = 'true'
      |                    GROUP BY
      |                            app_id,
      |                            disposal_tag
      |                )
      |            )
      |    GROUP BY
      |            app_id
      |),
      |${logName} AS (
      |    SELECT  advertiser_id,
      |            app_name,
      |            a.app_id AS app_id,
      |            IF(b.app_id IS NOT NULL, b.app_type, 0) AS app_flag,
      |            a.union_imei AS union_imei,
      |            ip,
      |            IF(
      |                ip IS NOT NULL,
      |                IF(
      |                    SIZE(split(ip, '.')) = 4,
      |                    concat(split(ip, '.')[0], '.', split(ip, '.')[1], '.', split(ip, '.')[2]),
      |                    ip
      |                ),
      |                ip
      |            ) AS ipc,
      |            IF(
      |                ip IS NOT NULL,
      |                IF(
      |                    SIZE(split(ip, '.')) = 4,
      |                    concat(split(ip, '.')[0], '.', split(ip, '.')[1]),
      |                    ip
      |                ),
      |                ip
      |            ) AS ipb,
      |            device_model,
      |            bill_type,
      |            event_type,
      |            invalid_event,
      |            req_id,
      |            rit,
      |            ot,
      |            click_time_timestamp,
      |            at_time_timestamp,
      |            send_time_timestamp,
      |            IF(
      |                send_time_timestamp IS NOT NULL
      |                AND click_time_timestamp IS NOT NULL,
      |                click_time_timestamp - send_time_timestamp,
      |                NULL
      |            ) AS click_send_tdiff,
      |            IF(
      |                at_time_timestamp IS NOT NULL
      |                AND click_time_timestamp IS NOT NULL,
      |                at_time_timestamp - click_time_timestamp,
      |                NULL
      |            ) AS convert_click_tdiff,
      |            IF(
      |                at_time_timestamp IS NOT NULL
      |                AND send_time_timestamp IS NOT NULL,
      |                at_time_timestamp - send_time_timestamp,
      |                NULL
      |            ) AS convert_send_tdiff,
      |            fraud_type,
      |            external_action,
      |            landing_type,
      |            is_search,
      |            is_reward_ads,
      |            ac,
      |            city_id,
      |            pay_amount,
      |            advertiser_first_industry_id,
      |            advertiser_second_industry_id,
      |            hour,
      |            date,
      |            room_id,
      |            ecpm_rmb,
      |            ecpm_bin
      |    FROM    (
      |                SELECT  union_imei
      |                FROM    union_anti.dwi_union_antispam_ecommerce_convert_uimei_log
      |                WHERE   p_date = '${date}'
      |                AND     hour = '${hour}'
      |            ) c
      |    INNER JOIN
      |            (
      |                SELECT  advertiser_id,
      |                        app_name,
      |                        app_id,
      |                        union_uuid AS union_imei,
      |                        get_json_object(external_json, '$$.client_ip') AS ip,
      |                        get_json_object(external_json, '$$.device_model') AS device_model,
      |                        get_json_object(external_json, '$$.room_id') AS room_id,
      |                        bill_type,
      |                        IF(ecpm IS NULL, 0.0, ecpm) / 100000 AS ecpm_rmb,
      |                        CAST(IF(ecpm IS NULL, 0.0, ecpm) / 100000 / 100 AS INT) * 100 AS ecpm_bin,
      |                        event_type,
      |                        invalid_event,
      |                        req_id,
      |                        rit,
      |                        ot,
      |                        client_time,
      |                        CAST(at AS BIGINT) AS at_time_timestamp,
      |                        CAST(get_json_object(external_json, '$$.click_at') AS BIGINT) AS click_time_timestamp,
      |                        CAST(get_json_object(external_json, '$$.conv_time') AS BIGINT) AS convert_time_timestamp,
      |                        IF(ot IS NOT NULL, CAST(ot AS BIGINT), NULL) AS send_time_timestamp,
      |                        pt,
      |                        convert_type,
      |                        fraud_type,
      |                        external_action,
      |                        landing_type,
      |                        is_search,
      |                        is_reward_ads,
      |                        ac,
      |                        city_id,
      |                        pay_amount,
      |                        advertiser_first_industry_id,
      |                        advertiser_second_industry_id,
      |                        date,
      |                        hour
      |                FROM    ad_dm.ad_stats_hourly
      |                WHERE   biz_line = 'union'
      |                AND     rit > 800000000
      |                AND     date >= FROM_UNIXTIME(UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize-1}, 'yyyyMMdd')
      |                AND     date <= '${date}'
      |                AND     concat(date, ' ', hour) >= FROM_UNIXTIME(UNIX_TIMESTAMP('${date} ${hour}', 'yyyyMMdd HH') - 3600 * ${windowSize-1}, 'yyyyMMdd HH')
      |                AND     concat(date, ' ', hour) <= concat('${date}', ' ', '${hour}')
      |                AND     label = 'convert'
      |            ) a
      |    ON      c.union_imei = a.union_imei
      |    LEFT JOIN
      |            (
      |                SELECT  app_id,
      |                        app_type
      |                FROM    reward_app_id
      |            ) b
      |    ON      a.app_id = b.app_id
      |)
      |""".stripMargin

    sqlQuery
  }
}
