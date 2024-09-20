
-- DIM维度层代码
DROP TABLE IF EXISTS gmall.tmp_dim_user_zip;
CREATE EXTERNAL TABLE gmall.tmp_dim_user_zip
(
    `id`           STRING COMMENT '用户id',
    `login_name`   STRING COMMENT '用户名称',
    `nick_name`    STRING COMMENT '用户昵称',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gmall/dim/tmp_dim_user_zip/'
    TBLPROPERTIES ('parquet.compression' = 'snappy');


SHOW PARTITIONS gmall.tmp_dim_user_zip ;
SELECT * FROM gmall.tmp_dim_user_zip WHERE dt = '9999-12-31' ;

-- todo：历史数据入库
SET hive.exec.dynamic.partition.mode=nonstrict ;
INSERT OVERWRITE TABLE gmall.tmp_dim_user_zip PARTITION (dt)
SELECT
    id, login_name, nick_name
     -- 数据脱敏，直接使用md5完成
     , md5(name) AS name, md5(phone_num) AS phone_num, md5(email) AS email
     , user_level, birthday, gender, create_time, operate_time
     , '2024-06-18' AS start_date
     , '9999-12-31' AS end_date
     , '9999-12-31' AS dt
FROM gmall.tmp_ods_user_info_inc
WHERE dt = '2024-06-18'
;
-- 我们把数据量大、缓慢变化的数据做成拉链表，考虑到数据量大，不适合采用全量采集，所以选择新增及变化采集，首先我们初始化拉链表，新增两个字段，开始时间和结束时间，
-- 然后就是制作当日用户变化表、包含新增及变化的数据，
-- 最新数据，存储9999-12-31 分区中
-- 过期数据，存储修改前一天分区中，比如2024 -09-18 修改数据，以前数据存储2024-09-17
-- 比如：用户维度表（用户数量超过100万），商品维度表（商品数量超过100万）



-- todo: 每日数据，拉链操作
SET hive.exec.dynamic.partition.mode=nonstrict ;
SET hive.stats.autogather=false ;
WITH
    old_data AS (
        -- 1. 获取用户维度表中，历史用户数据6.18
        SELECT
            id, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time
             , start_date, end_date
             , dt
        FROM gmall.tmp_dim_user_zip
        WHERE dt = '9999-12-31'
    )
   , new_data AS (
        -- 2. ODS层，增量同步数据6.19
        SELECT
            id, login_name, nick_name, md5(name) AS name, md5(phone_num) AS phone_num, md5(email) AS email
             , user_level, birthday, gender, create_time, operate_time
             , '2024-06-19' AS start_date, '9999-12-31' AS end_date
             , '9999-12-31' AS dt
        FROM gmall.tmp_ods_user_info_inc
        WHERE dt = '2024-06-19'
    )
   , unin_data AS (
        -- 3. 合并所有数据
        SELECT * FROM old_data
        UNION
        SELECT * FROM new_data
    )
   , rank_data AS (
        SELECT
            id, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time
             , row_number() over (partition by id order by start_date desc) AS rk
             , start_date, end_date, dt
        FROM unin_data
    )
INSERT OVERWRITE TABLE gmall.tmp_dim_user_zip PARTITION (dt)
SELECT
    id, login_name, nick_name, name, phone_num, email, user_level, birthday, gender, create_time, operate_time
     , start_date
     -- 编号为1时，数据为最新的，结束日期：9999-12-31，否则为过期数据，结束日期：日期 - 1 day
     , if(rk = 1, end_date, date_sub('2024-06-19', 1)) AS end_date
     -- 拉链表数据存储分区为 数据结束日期
     , if(rk = 1, end_date, date_sub('2024-06-19', 1)) AS dt
FROM rank_data
;

