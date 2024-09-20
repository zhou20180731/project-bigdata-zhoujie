
/*
    数据来源于：
    ods_sku_info_full（全量）
    和  ods_spu_info_full（全量） 合并
    和  ods_base_category3_full（全量） 合并
    和  ods_base_category2_full（全量） 合并
    和  ods_base_category1_full（全量） 合并
    和  ods_base_trademark_full（全量） 合并
    和  ods_sku_attr_value_full（全量） 合并
    和  ods_sku_sale_attr_value_full（全量） 合并

*/
USE gmall;
SET hive.stats.autogather=false;
-- todo 数据加载： 1 商品维度表

with
    --商品信息表
    sku As
        (
            select
                id,
                spu_id,
                price,
                sku_name,
                sku_desc,
                weight,
                tm_id,
                category3_id,
                sku_default_img,
                is_sale,
                create_time
            from gmall.ods_sku_info_full
            where dt='2024-06-19'
        ),
    --商品表
    spu As
        (
            select
                id,
                spu_name
            from gmall.ods_spu_info_full
            where dt='2024-06-19'
        ),
 -- 三级分类
    category3 AS (
        SELECT id,
               name,
               category2_id
        FROM gmall.ods_base_category3_full
        WHERE dt='2024-06-19'
        ),
    -- 二级分类
        category2 AS (
    SELECT id,
           name,
           category1_id
    FROM gmall.ods_base_category2_full
    WHERE dt='2024-06-19'
   ),
    -- 一级分类
         category1 AS (
    SELECT id,
           name
    FROM gmall.ods_base_category1_full
    WHERE dt='2024-06-19'
),
    --品牌表
    tm As
        (
            select
                id,
                tm_name
            from gmall.ods_base_trademark_full
            where dt='2024-06-19'
        ),
    --sku平台属性关联表
    attr AS
        (
            select
                sku_id,
                collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
            from gmall.ods_sku_attr_value_full
            where dt='2024-06-19'
            group by sku_id
        ),
  --平台销售属性关联表
    sale_attr As
        (
            select
                sku_id,
                collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
            from ods_sku_sale_attr_value_full
            where dt='2024-06-18'
            group by sku_id
        )
insert overwrite table dim_sku_full partition(dt='2024-06-19')
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
    category3.name,
    category3.category2_id,
    category2.name,
    category2.category1_id,
    category1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
         left join spu on spu.id=sku.spu_id
         left join category3 on category3.id = sku.category3_id
         left join category2 on category2.id = category3.category2_id
         left join category1 on category1.id = category2.category1_id
         left join tm on tm.id=sku.tm_id
         left join attr on attr.sku_id =sku.id
         left join sale_attr on sale_attr.sku_id= sku.id;

-- todo 数据加载： 2 优惠券维度表

/*
    数据来源于：
    ods_coupon_info_full（全量）
    和  ods_base_dic_full（全量） 合并
*/

WITH ci AS(
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
    FROM  gmall.ods_coupon_info_full
    WHERE dt='2024-06-19'
),

     coupon_dic As (
         SELECT
             dic_code,
             dic_name
         FROM gmall.ods_base_dic_full
         WHERE dt='2024-06-19'
           AND  parent_code='32'
     ),
     range_dic As (
         select  dic_code,
                 dic_name
         FROM gmall.ods_base_dic_full
         WHERE dt='2024-06-19'
           AND  parent_code='33'
     )
INSERT OVERWRITE TABLE
 dim_coupon_full PARTITION(dt='2024-06-19')
select
    ci.id,
    ci.coupon_name,
    ci.coupon_type,
    coupon_dic.dic_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    case ci.coupon_type
        when '3201' then concat('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
        when '3202' then concat('满', ci.condition_num, '件打', 10 * (1 - ci.benefit_discount), '折')
        when '3203' then concat('减', ci.benefit_amount, '元')
        end as benefit_rule,
    ci.create_time,
    ci.range_type,
    range_dic.dic_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time

from ci
         left join coupon_dic on ci.coupon_type = coupon_dic.dic_code
         left join range_dic on ci.range_type = range_dic.dic_code;

-- 测试
SHOW PARTITIONS gmall.dim_coupon_full;
SELECT *
FROM gmall.dim_coupon_full
WHERE dt = '2024-06-19'
LIMIT 10;


-- todo 数据加载： 3 活动维度表
/*
    数据来源于：
    ods_activity_rule_full（全量）
    和  ods_activity_info_full（全量） 合并
    和  ods_base_dic_full（全量） 合并
*/


set hive.exec.dynamic.partition = true; -- 启用动态分区
set hive.exec.dynamic.partition.mode = nonstrict; -- 允许动态分区模式为非严格
set hive.stats.autogather = false; -- 禁用统计信息自动收集

WITH
    activity_rule AS (
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
            ods_activity_rule_full
        WHERE
            dt='2024-06-19'
    ),
    activity_info AS (
        SELECT
            id,
            activity_name,
            activity_type,
            activity_desc,
            start_time,
            end_time,
            create_time
        FROM
            ods_activity_info_full
        WHERE
            dt='2024-06-19'
    ),
    type_dic AS (
        SELECT dic_code
             , dic_name
        FROM ods_base_dic_full
        WHERE dic_code = '31'
    )

INSERT OVERWRITE TABLE dim_activity_full PARTITION(dt='2024-06-19')
SELECT activity_rule.id,
       activity_info.id,
       activity_name,
       type_dic.dic_code,
       type_dic.dic_name,

       activity_desc,
       start_time,
       end_time,
       create_time,
       condition_amount,

       condition_num,
       benefit_amount,
       benefit_discount,
       CASE
           WHEN activity_info.activity_type = '3201' then concat('满', condition_amount, '元 减', benefit_amount, '元')
           WHEN activity_info.activity_type = '3202'
               THEN concat('满', condition_num, '件 打', 10 * (1 - benefit_discount), '折')
           WHEN activity_info.activity_type = '3202' THEN concat('减', benefit_amount, '元')
           END benefit_rule,
       benefit_level
FROM activity_info
         INNER JOIN activity_rule ON activity_info.id = activity_rule.activity_id
         INNER JOIN type_dic ON activity_info.activity_type = type_dic.dic_code
;

-- todo 数据加载： 4. 地区维度表
/*
    数据来源于：
        ods_base_province_full（全量）
    和  ods_base_region_full（全量） 合并
*/
SET hive.stats.autogather=false;
WITH
    -- a. 省份数据
    province AS (
        SELECT
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        FROM gmall.ods_base_province_full
        WHERE dt = '2024-06-19'
    )
    -- b. 地区数据
    , region AS (
        SELECT
            id,
            region_name
        FROM gmall.ods_base_region_full
        WHERE dt = '2024-06-19'
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
SELECT * FROM gmall.dim_province_full WHERE dt = '2024-06-18' LIMIT 10 ;



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
-- hdfs dfs -put date_info.txt /warehouse/gmall/tmp/tmp_dim_date_info/

--（3）执行以下语句将其导入时间维度表
INSERT OVERWRITE TABLE gmall.dim_date
SELECT * FROM gmall.dim_date
UNION
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

/*
    数据来源于：
    ods_activity_rule_full（全量）
    和  ods_activity_info_full（全量） 合并
    和  ods_base_dic_full（全量） 合并
*/






















