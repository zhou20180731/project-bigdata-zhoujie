
SET hive.exec.dynamic.partition = true; -- 启用动态分区
SET hive.exec.dynamic.partition.mode = nonstrict; -- 允许动态分区模式为非严格
SET hive.stats.autogather = false; -- 禁用统计信息自动收集

-- todo 数据加载： 1 商品维度表

WITH
    sku As
        (
            SELECT
                id,
                price,
                sku_name,
                sku_desc,
                weight,
                is_sale,
                spu_id,
                category3_id,
                tm_id,
                create_time
            FROM gmall.ods_sku_info_full
            WHERE  dt='2024-06-19'
        ),
    spu As
        (
            SELECT
                id,
                spu_name
            FROM gmall.ods_spu_info_full
            WHERE  dt='2024-06-19'
        ),
    c3 As
        (
            SELECT
                id,
                name,
                category2_id
            FROM gmall.ods_base_category3_full
            WHERE  dt='2024-06-19'
        ),
    c2 As
        (
            SELECT
                id,
                name,
                category1_id
            FROM gmall.ods_base_category2_full
            WHERE  dt='2024-06-19'
        ),
    c1 As
        (
            SELECT
                id,
                name
            FROM gmall.ods_base_category1_full
            WHERE  dt='2024-06-19'
        ),
    tm As
        (
            SELECT
                id,
                tm_name
            FROM gmall.ods_base_trademark_full
            WHERE  dt='2024-06-19'
        ),
    attr As
        (
            SELECT
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            FROM gmall.ods_sku_attr_value_full
            WHERE  dt='2024-06-19'
            group by sku_id
        ),
    sale_attr As
        (
            SELECT
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            FROM gmall.ods_sku_sale_attr_value_full
            WHERE  dt='2024-06-19'
            group by sku_id
        )
insert overwrite table gmall.dim_sku_full partition(dt='2024-06-19')
SELECT
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
FROM sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;



-- todo 数据加载： 2 优惠券维度表
WITH 
    ci As (
    SELECT
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    FROM gmall.ods_coupon_info_full
    WHERE  dt='2024-06-19'
),
     coupon_dic As (
         SELECT
             dic_code,
             dic_name
         FROM gmall.ods_base_dic_full
         WHERE  dt='2024-06-19'
           and parent_code='32'
     ),
     range_dic As (
         SELECT
             dic_code,
             dic_name
         FROM gmall.ods_base_dic_full
         WHERE  dt='2024-06-19'
           and parent_code='33'
     )
insert overwrite table gmall.dim_coupon_full partition(dt='2024-06-19')
SELECT
    ci.id,
    ci.coupon_name,
    ci.coupon_type,
    coupon_dic.dic_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    cAse ci.coupon_type
        when '3201' then concat('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
        when '3202' then concat('满', ci.condition_num, '件打', 10 * (1 - ci.benefit_discount), '折')
        when '3203' then concat('减', ci.benefit_amount, '元')
        end As benefit_rule,
    ci.create_time,
    ci.range_type,
    range_dic.dic_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time

FROM ci
         left join coupon_dic on ci.coupon_type = coupon_dic.dic_code
         left join range_dic on ci.range_type = range_dic.dic_code;




-- todo 数据加载： 3 活动维度表
set hive.exec.dynamic.partition = true; -- 启用动态分区
set hive.exec.dynamic.partition.mode = nonstrict; -- 允许动态分区模式为非严格
set hive.stats.autogather = false; -- 禁用统计信息自动收集

WITH
    rule As (
        SELECT
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        FROM
            gmall.ods_activity_rule_full
        WHERE 
            dt='2024-06-19'
    ),
    info As (
        SELECT
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        FROM
            gmall.ods_activity_info_full
        WHERE 
            dt='2024-06-19'
    ),
    dic As (
        SELECT
            dic_code,
            dic_name
        FROM
            gmall.ods_base_dic_full
        WHERE 
            dt='2024-06-19'
          AND parent_code='31'
    )
INSERT OVERWRITE TABLE gmall.dim_activity_full PARTITION(dt='2024-06-19')
SELECT
    rule.id,
    info.id,
    info.activity_name,
    rule.activity_type,
    dic.dic_name,
    info.activity_desc,
    info.start_time,
    info.end_time,
    info.create_time,
    rule.condition_amount,
    rule.condition_num,
    rule.benefit_amount,
    rule.benefit_discount,
    CAsE
        WHEN rule.activity_type = '3101' THEN CONCAT('满', rule.condition_amount, '元减', rule.benefit_amount, '元')
        WHEN rule.activity_type = '3102' THEN CONCAT('满', rule.condition_num, '件打', 10 * (1 - rule.benefit_discount), '折')
        WHEN rule.activity_type = '3103' THEN CONCAT('打', 10 * (1 - rule.benefit_discount), '折')
        END As benefit_rule,
    rule.benefit_level
FROM
    rule
        LEFT JOIN
    info ON rule.activity_id = info.id
        LEFT JOIN
    dic ON rule.activity_type = dic.dic_code;


-- todo 数据加载： 4. 地区维度表
/*
    数据来源于：
        ods_base_province_full（全量） 和  ods_base_region_full（全量） 合并
*/
SET hive.stats.autogather=false;
WITH
   -- a. 省份数据
    province As (
        SELECT
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        FROM gmall.ods_base_province_full
        WHERE  dt = '2024-06-19'
    )
   -- b. 地区数据
   , region As (
    SELECT
        id,
        region_name
    FROM gmall.ods_base_region_full
    WHERE  dt = '2024-06-19'
)
INSERT OVERWRITE TABLE gmall.dim_province_full PARTITION(dt = '2024-06-19')
-- c. 省份数据关联地区数据，按照region_id关联
SELECT
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
FROM province
         LEFT JOIN region ON province.region_id = region.id;

-- 测试
SHOW PARTITIONS gmall.dim_province_full;
SELECT * FROM gmall.dim_province_full WHERE  dt = '2024-06-19' LIMIT 10 ;



-- todo 数据加载： 5 日期维度表
/*
    数据来源于：
        通常情况下，时间维度表的数据并不是来自于业务系统，而是手动写入，并且由于时间维度表数据的可预见性，
    无须每日导入，一般可一次性导入一年的数据。
*/
--（1）创建临时表
DROP TABLE IF EXISTS gmall.tmp_dim_date_info;
CREATE TABLE gmall.tmp_dim_date_info (
                                         `date_id` STRING COMMENT '日',
                                         `week_id` STRING COMMENT '周ID',
                                         `week_day` STRING COMMENT '周几',
                                         `day` STRING COMMENT '每月的第几天',
                                         `month` STRING COMMENT '第几月',
                                         `quarter` STRING COMMENT '第几季度',
                                         `year` STRING COMMENT '年',
                                         `is_workday` STRING COMMENT '是否是工作日',
                                         `holiday_id` STRING COMMENT '节假日'
) COMMENT '时间维度表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/warehouse/gmall/tmp/tmp_dim_date_info/';

-- （2）将数据文件上传到HFDS上临时表路径/warehouse/gmall/tmp/tmp_dim_date_info
-- hdfs dfs -put date_info_2024.txt /warehouse/gmall/tmp/tmp_dim_date_info/

-- hdfs dfs -put date_info.txt /warehouse/gmall/tmp/tmp_dim_date_info/


--（3）执行以下语句将其导入时间维度表
INSERT OVERWRITE TABLE gmall.dim_date
-- SELECT * FROM gmall.dim_date
-- UNION
SELECT date_id,
       week_id,
       week_day,
       day,
       month,
       quarter,
       year,
       is_workday,
       holiday_id
FROM gmall.tmp_dim_date_info;

-- 测试
SELECT * FROM gmall.dim_date ;



-- todo 数据加载： 6 用户维度表
WITH
--     用户信息表
user_info As (
    SELECT
        id,
        login_name,
        nick_name,
        pAsswd,
        name,
        phone_num,
        email,
        head_img,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        status,
        '2024-06-18' As start_date,
        '9999-12-31' As end_date
    FROM gmall.ods_user_info_inc
    WHERE  dt='2024-06-18'
)
INSERT INTO TABLE gmall.dim_user_zip PARTITION (dt='2024-06-18')
SELECT
    id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date
FROM user_info;


------临时用户表
WITH
--     用户信息表
user_info As (
    SELECT
        id,
        login_name,
        nick_name,
        pAsswd,
        name,
        phone_num,
        email,
        head_img,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        status,
        '2024-06-18' As start_date,
        '9999-12-31' As end_date
    FROM gmall.ods_user_info_inc
    WHERE  dt='2024-06-18'
)
INSERT INTO TABLE gmall.tmp_dim_user_zip PARTITION (dt='2024-06-18')
SELECT
    id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date
FROM user_info;
