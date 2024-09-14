
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、男性和女性每日的购物总金额统计
/*
从订单信息表（order_info）和用户信息表（user_info）中，分别统计每天男性和女性用户的订单总金额，如果当天男性或者女性没有购物，则统计结果为0。
*/
WITH
-- step1. 关联用户信息表，获取性别
    tmp1 AS (
        SELECT
            t1.order_id, t1.user_id, t1.create_date, t1.total_amount, t2.gender
        FROM hive_sql_zg6.order_info t1
                 LEFT JOIN hive_sql_zg6.user_info t2 ON t1.user_id = t2.user_id
    )
-- step2. 按日期分组，聚合统计
SELECT
    create_date
    , sum(if(gender = '男', total_amount, 0)) AS male_amount_day
    , sum(if(gender = '女', total_amount, 0)) AS female_amount_day
FROM tmp1
GROUP BY create_date
;


-- todo: 2）、订单金额趋势分析
/*
查询截止每天的最近3天内的订单金额总和以及订单金额日平均值，保留两位小数，四舍五入。
*/
WITH
-- step1. 统计每日订单金额
    tmp1 AS (
        SELECT
            create_date
             , sum(total_amount) AS amount_day
        FROM hive_sql_zg6.order_info
        GROUP BY create_date
    )
-- step2. 窗口计算
SELECT
    create_date
    , amount_day
    -- 最近3天内的订单金额总和
    , sum(amount_day) OVER (ORDER BY create_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_amount_3d
    -- 最近3天内订单金额日平均值
    , round(
        avg(amount_day) OVER (ORDER BY create_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2
    ) AS avg_amount_3d
FROM tmp1
;

-- todo: 3）、购买过商品1和商品2但是没有购买商品3的顾客
/*
从订单明细表(order_detail)中查询出所有购买过商品1和商品2，但是没有购买过商品3的用户。
*/
WITH
-- step1. 订单明细表 关联订单表，获取user_id
    tmp1 AS (
        SELECT
            t2.user_id, t1.sku_id
        FROM hive_sql_zg6.order_detail t1
        LEFT JOIN hive_sql_zg6.order_info t2 ON t1.order_id = t2.order_id
    )
-- step2. 用户分组，统计购买商品
    , tmp2 AS (
        SELECT
            user_id
             , collect_set(sku_id) AS sku_id_list
        FROM tmp1
        WHERE sku_id IN ('1', '2', '3')
        GROUP BY user_id
    )
-- step3. 按照条件过滤
SELECT user_id, sku_id_list
FROM tmp2
WHERE array_contains(sku_id_list, '1')
    AND array_contains(sku_id_list, '2')
    AND ! array_contains(sku_id_list, '3')
;

-- 函数：
DESC FUNCTION array_contains;
/*
array_contains(array, value) - Returns TRUE if the array contains value.
*/
SELECT
    array_contains(`array`(1, 2, 3, 4, 5), 5) AS x1
    , array_contains(`array`(1, 2, 3, 4, 5), 9) AS x2
;


-- todo: 4）、统计每日商品1和商品2销量的差值
/*
从订单明细表（order_detail）中统计每天商品1和商品2销量（件数）的差值（商品1销量-商品2销量）。
*/
WITH
-- step1. 过滤，分组，聚合
    tmp1 AS (
        SELECT
            create_date
             , sum(if(sku_id = '1', sku_num, 0)) AS sku1_num_day
             , sum(if(sku_id = '2', sku_num, 0)) AS sku2_num_day
        FROM hive_sql_zg6.order_detail
        WHERE sku_id IN ('1', '2')
        GROUP BY create_date
    )
-- step2. 计算差值
SELECT
    create_date, sku1_num_day, sku2_num_day
    , sku1_num_day - sku2_num_day AS sku_num_diff
FROM tmp1
;

/*
    谓词下推 predicate pushdown
        尽量将过滤操作前移，先过滤后计算
*/
SET hive.optimize.ppd = true ;


-- todo: 5）、查询出每个用户的最近三笔订单
/*
从订单信息表（order_info）中查询出每个用户的最近三笔订单。
*/
/*
    排序开窗函数：
        row_number：1,2,3,4,5,...
        rank：1,2,2,4,5,6...
        dense_rank：1,2,2,3,4,5...
*/
WITH
-- step1. 加序号
    tmp1 AS (
        SELECT
            *,
            row_number() over (PARTITION BY user_id ORDER BY create_date DESC) AS rk
        FROM hive_sql_zg6.order_info
    )
-- step3. 过滤前3
SELECT *
FROM tmp1
WHERE rk <= 3
;


