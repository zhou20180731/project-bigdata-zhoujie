#!/bin/bash

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi


#3 活动维度表
#    数据来源于：
#        ods_activity_rule_full（全量） 和
#        ods_activity_info_full（全量） 和
#        ods_base_dic_full（全量） 合并
dim_activity_sql=
WITH
    rule AS (
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
            dt='${do_date}'
    ),
    info AS (
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
            dt='${do_date}'
    ),
    dic AS (
        SELECT
            dic_code,
            dic_name
        FROM
            gmall.ods_base_dic_full
        WHERE
            dt='${do_date}'
          AND parent_code='31'
    )
INSERT OVERWRITE TABLE gmall.dim_activity_full PARTITION(dt='${do_date}')
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
    CASE
        WHEN rule.activity_type = '3101' THEN CONCAT('满', rule.condition_amount, '元减', rule.benefit_amount, '元')
        WHEN rule.activity_type = '3102' THEN CONCAT('满', rule.condition_num, '件打', 10 * (1 - rule.benefit_discount), '折')
        WHEN rule.activity_type = '3103' THEN CONCAT('打', 10 * (1 - rule.benefit_discount), '折')
        END AS benefit_rule,
    rule.benefit_level
FROM
    rule
        LEFT JOIN
    info ON rule.activity_id = info.id
        LEFT JOIN
    dic ON rule.activity_type = dic.dic_code;
"

#2 优惠券维度表
#    数据来源于：
#        ods_coupon_info_full（全量） 和
#        ods_base_dic_full（全量） 合并
dim_coupon_sql="
WITH
    ci AS (
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
    WHERE dt='2024-09-14'
),
     base_dic AS (
         SELECT
             dic_code,
             dic_name
         FROM gmall.ods_base_dic_full
         WHERE dt='2024-09-14'
           AND parent_code IS NOT NULL
     )
INSERT OVERWRITE TABLE gmall.dim_coupon_full PARTITION(dt='2024-09-14')
SELECT
    ci.id,
    ci.coupon_name,
    ci.coupon_type,
    base_dic.dic_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    CASE ci.coupon_type
        WHEN '3201' THEN concat('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
        WHEN '3202' THEN concat('满', ci.condition_num, '件打', 10 * (1 - ci.benefit_discount), '折')
        WHEN '3203' THEN concat('减', ci.benefit_amount, '元')
        END AS benefit_rule,
    ci.create_time,
    ci.range_type,
    base_dic.dic_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time
FROM ci
         LEFT JOIN base_dic ON (ci.coupon_type = base_dic.dic_code
                                   OR ci.range_type = base_dic.dic_code);
"

#4. 地区维度表
#    数据来源于：
#        ods_base_province_full（全量） 和
#        ods_base_region_full（全量） 合并
#省份数据关联地区数据，按照region_id关联
dim_province_sql="
SET hive.stats.autogather=false;
WITH
    province AS (
        SELECT
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        FROM gmall.ods_base_province_full
        WHERE dt = '${do_date}'
    )
    , region AS (
        SELECT
            id,
            region_name
        FROM gmall.ods_base_region_full
        WHERE dt = '${do_date}'
    )
INSERT OVERWRITE TABLE gmall.dim_province_full PARTITION(dt = '${do_date}')
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
"

#5 商品维度表
#    数据来源于：
#        ods_sku_info_full（全量） 和
#        ods_spu_info_full（全量） 和
#        ods_base_category3_full（全量） 和
#        ods_base_category2_full（全量） 和
#        ods_base_category1_full（全量） 和
#        ods_base_trademark_full（全量） 和
#        ods_sku_attr_value_full（全量） 和
#        ods_sku_sale_attr_value_full（全量） 合并
dim_sku_sql="
WITH
    sku as
        (
            select
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
            from gmall.ods_sku_info_full
            where dt='${do_date}'
        ),
    spu as
        (
            select
                id,
                spu_name
            from gmall.ods_spu_info_full
            where dt='${do_date}'
        ),
    c3 as
        (
            select
                id,
                name,
                category2_id
            from gmall.ods_base_category3_full
            where dt='${do_date}'
        ),
    c2 as
        (
            select
                id,
                name,
                category1_id
            from gmall.ods_base_category2_full
            where dt='${do_date}'
        ),
    c1 as
        (
            select
                id,
                name
            from gmall.ods_base_category1_full
            where dt='${do_date}'
        ),
    tm as
        (
            select
                id,
                tm_name
            from gmall.ods_base_trademark_full
            where dt='${do_date}'
        ),
    attr as
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from gmall.ods_sku_attr_value_full
            where dt='${do_date}'
            group by sku_id
        ),
    sale_attr as
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from gmall.ods_sku_sale_attr_value_full
            where dt='${do_date}'
            group by sku_id
        )
insert overwrite table gmall.dim_sku_full partition(dt='${do_date}')
select
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
from sku
         left join spu on sku.spu_id=spu.id
         left join c3 on sku.category3_id=c3.id
         left join c2 on c3.category2_id=c2.id
         left join c1 on c2.category1_id=c1.id
         left join tm on sku.tm_id=tm.id
         left join attr on sku.id=attr.sku_id
         left join sale_attr on sku.id=sale_attr.sku_id;
"

#6 用户维度表
#    数据来源于：
#        ods_user_info_inc（增量）
dim_user_sql="
WITH
--     用户信息表
user_info AS (
    SELECT
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
        '${do_date}' AS start_date,
        '9999-12-31' AS end_date
    FROM gmall.ods_user_info_inc
    WHERE dt='${do_date}'
)
INSERT INTO TABLE gmall.dim_user_zip PARTITION (dt='${do_date}')
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
"

case $1 in
  "dim_activity"){
    hive -e "${dim_activity_sql}"
  };;
  "dim_coupon"){
    hive -e "${dim_coupon_sql}"
  };;
  "dim_province"){
    hive -e "${dim_province_sql}"
  };;
  "dim_sku"){
    hive -e "${dim_sku_sql}"
  };;
  "dim_user"){
    hive -e "${dim_user_sql}"
  };;
  "all"){
    hive -e "
    ${dim_activity_sql};
    ${dim_coupon_sql};
    ${dim_province_sql};
    ${dim_sku_sql};
    ${dim_user_sql};
    "
  };;
esac