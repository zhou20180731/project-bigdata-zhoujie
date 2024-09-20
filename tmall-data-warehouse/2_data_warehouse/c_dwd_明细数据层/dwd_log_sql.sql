
/*
    `province_id`    STRING COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否新用户',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
*/
WITH
    -- a. 日志表数据
    log AS (
        SELECT
             common.ar AS `ar`
             , common.ba AS `brand`
             , common.ch AS `channel`
             , common.is_new AS `is_new`
             , common.md AS `model`
             , common.mid AS `mid_id`
             , common.os AS `operate_system`
             , common.uid AS `user_id`
             , common.vc AS `version_code`
             , page.item AS `page_item`
             , page.item_type AS `page_item_type`
             , page.last_page_id AS `last_page_id`
             , page.page_id AS `page_id`
             , page.source_type AS `source_type`
             , date_format(from_utc_timestamp(ts,'GMT+8'), 'yyyy-MM-dd') AS `date_id`
             , date_format(from_utc_timestamp(ts,'GMT+8'),' yyyy-MM-dd HH:mm:ss') AS `view_time`
             , page.during_time AS `during_time`
        FROM gmall.ods_log_inc
        WHERE dt = '2024-09-11'
    )
   -- b. 获取每个会话起始点：last_page_id IS NULL
   , session AS (
        SELECT
            *
             , if(last_page_id IS NULL, concat(mid_id, '-', view_time), NULL) AS session_start_point
        FROM log
        WHERE page_id IS NOT NULL
        ORDER BY mid_id, view_time
    )
   -- c. 使用last_value开窗函数，给每个会话加上id
   , tmp AS (
        SELECT
            *
             , last_value(session_start_point, true) over(PARTITION BY mid_id ORDER BY view_time) AS session_id
        FROM session
    )
   -- d. 省份信息表
   , province AS (
        SELECT
            id AS province_id
            , area_code
        FROM gmall.ods_base_province_full
        WHERE dt = '2024-06-18'
    )
-- todo 插入表
INSERT OVERWRITE TABLE gmall.dwd_traffic_page_view_inc PARTITION (dt = '2024-09-13')
SELECT
     province.`province_id`
    , `brand`
    , `channel`
    , `is_new`
    , `model`
    , `mid_id`
    , `operate_system`
    , `user_id`
    , `version_code`
    , `page_item`
    , `page_item_type`
    , `last_page_id`
    , `page_id`
    , `source_type`
    , `date_id`
    , `view_time`
    , `session_id`
    , `during_time`
FROM tmp
    LEFT JOIN province ON tmp.ar = province.area_code
;


WITH
    -- a. 日志表数据
    log AS (
        SELECT
            common.ar AS `ar`
             , common.ba AS `brand`
             , common.ch AS `channel`
             , common.is_new AS `is_new`
             , common.md AS `model`
             , common.mid AS `mid_id`
             , common.os AS `operate_system`
             , common.uid AS `user_id`
             , common.vc AS `version_code`
             , page.item AS `page_item`
             , page.item_type AS `page_item_type`
             , page.last_page_id AS `last_page_id`
             , page.page_id AS `page_id`
             , page.source_type AS `source_type`
             , date_format(from_utc_timestamp(ts,'GMT+8'), 'yyyy-MM-dd') AS `date_id`
             , date_format(from_utc_timestamp(ts,'GMT+8'),' yyyy-MM-dd HH:mm:ss') AS `view_time`
             , page.during_time AS `during_time`
        FROM gmall.ods_log_inc
        WHERE dt = '2024-06-18'
    )
    -- b. 获取每个会话起始点：last_page_id IS NULL
    , session AS (
        SELECT
             *
             , if(last_page_id IS NULL, concat(mid_id, '-', view_time), NULL) AS session_start_point
        FROM log
        WHERE page_id IS NOT NULL
        ORDER BY mid_id, view_time
    )
-- c. 使用last_value开窗函数，给每个会话加上id
SELECT
    mid_id, view_time, page_id, last_page_id
    -- , session_start_point
    , last_value(session_start_point, true) over(PARTITION BY mid_id ORDER BY view_time) AS session_id
FROM session
;

DESC FUNCTION last_value;

