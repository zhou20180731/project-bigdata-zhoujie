

-- ODS 层数据
-- ======================================================================
--               todo： 10.用户表：user_info（每日，新增变化）
-- ======================================================================
DROP TABLE IF EXISTS gmall.tmp_ods_user_info_inc;
CREATE EXTERNAL TABLE gmall.tmp_ods_user_info_inc(
    `id` STRING COMMENT '用户id',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `passwd` STRING COMMENT '用户密码',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `head_img` STRING COMMENT '头像',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `status` STRING COMMENT '状态'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/tmp_ods_user_info_inc/';

SHOW PARTITIONS gmall.tmp_ods_user_info_inc;
SELECT * FROM gmall.tmp_ods_user_info_inc WHERE dt = '2024-06-19' ;

/*
-- todo 增量数据同步：历史数据同步
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/tmall_tmp \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/tmp_user_info_inc/2024-06-18 \
--delete-target-dir \
--query "SELECT
    id,
    login_name,
    nick_name,
    passwd,
    name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    status
FROM user_info
WHERE date_format(create_time, '%Y-%m-%d') <= '2024-06-18'
  AND (operate_time IS NULL OR date_format(operate_time, '%Y-%m-%d') <= '2024-06-18')
  AND \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'


-- todo 增量数据同步：每日数据同步
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/tmall_tmp \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/tmp_user_info_inc/2024-06-19 \
--delete-target-dir \
--query "SELECT
    id,
    login_name,
    nick_name,
    passwd,
    name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    status
FROM user_info
WHERE (date_format(create_time, '%Y-%m-%d') = '2024-06-19'
        OR date_format(operate_time, '%Y-%m-%d') = '2024-06-19'
      )
      and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'

 */


-- todo 10.用户表：user_info（每日，新增变化）
LOAD DATA INPATH '/origin_data/gmall/tmp_user_info_inc/2024-06-18'
    OVERWRITE INTO TABLE gmall.tmp_ods_user_info_inc PARTITION (dt = '2024-06-18');


LOAD DATA INPATH '/origin_data/gmall/tmp_user_info_inc/2024-06-19'
    OVERWRITE INTO TABLE gmall.tmp_ods_user_info_inc PARTITION (dt = '2024-06-19');


LOAD DATA INPATH '/origin_data/gmall/log/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_log_inc  PARTITION(dt='2024-06-18');