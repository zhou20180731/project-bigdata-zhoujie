
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、查询累积销量排名第二的商品
/*
查询订单明细表（order_detail）中销量（下单件数）排名第二的商品id，
	如果不存在返回null，如果存在多个排名第二的商品则需要全部返回
*/
/*
todo: 排序开窗函数
    row_number：不重复，连续，1、2、3、4、5、...
    rank：重复，跳跃，1、1、3、4、5、...
    dense_rank：重复，连续，，1、1、2、3、4、...
*/
WITH
-- step1. 按照商品分组，统计累计销量
    tmp1 AS (
        SELECT
            sku_id
             , sum(sku_num) AS sku_num_sum
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id
    )
-- step2. 加上序号：销量降序排序
    , tmp2 AS (
        SELECT
            sku_id, sku_num_sum
             , rank() over (order by sku_num_sum DESC) AS rnk
        FROM tmp1
    )
-- step3. 过滤获取销量第2商品
    , tmp3 AS (
        SELECT sku_id
        FROM tmp2
        WHERE rnk = 2
    )
-- step4. 关联右表，补充数据
SELECT sku_id
FROM tmp3
RIGHT JOIN (
    SELECT 1 AS x1
) tmp4 ON 1 = 1
;

-- todo：2）、查询至少连续三天下单的用户
/*
查询订单信息表(order_info)中最少连续3天下单的用户id
*/
/*
todo 思路：连续登录几天，每天日期与序号差值相同
    step1. 去重（考虑1天可能多次下单）
    step2. 序号（用户id分组，日期升序排序，row_number开窗）
    step3. 差值（计算日期与序号差值）
    step4. 分组、计数、过滤
    step5. 去重
*/
WITH
-- step1. 去重（考虑1天可能多次下单）
    tmp1 AS (
        SELECT user_id, create_date
        FROM hive_sql_zg6.order_info
        GROUP BY user_id, create_date
    )
-- step2. 序号（用户id分组，日期升序排序，row_number开窗）
    , tmp2 AS (
        SELECT
            user_id, create_date
            , row_number() OVER (PARTITION BY user_id ORDER BY create_date) AS rnk
        FROM tmp1
    )
-- step3. 差值（计算日期与序号差值）
    , tmp3 AS (
        SELECT
            user_id, create_date
            , rnk
            , date_sub(create_date, rnk) AS date_diff
        FROM tmp2
    )
-- step4. 分组、计数、过滤
    , tmp4 AS (
        SELECT
            user_id
    --     , count(user_id) AS days
    --     , collect_list(create_date) AS date_list
        FROM tmp3
        GROUP BY user_id, date_diff
        HAVING count(user_id) >= 3
    )
-- 5. 去重，考虑一个用户多次连续下单
SELECT
    DISTINCT user_id
FROM tmp4
;

-- todo: 3）、查询各品类销售商品的种类数及销量最高的商品
/*
从订单明细表(order_detail)统计各品类销售出的商品种类数及累积销量最好的商品
		品类ID（品类名称）、品类销售商品的种类数、销售量最好的商品、销售量
*/
WITH
    tmp1 AS (
        -- step1. 各个商品销量
        SELECT
            sku_id
             , sum(sku_num) AS sku_num_sum
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id
    )
    , tmp2 AS (
        -- step2. 商品品类维表
        SELECT
            t1.sku_id
             , t1.name AS sku_name
             , t2.category_id
             , t2.category_name
        FROM hive_sql_zg6.sku_info t1
            LEFT JOIN hive_sql_zg6.category_info t2 on t1.category_id = t2.category_id
    )
    , tmp3 AS (
        -- step3. 关联join
        SELECT
            tt1.sku_id
             , tt2.sku_name
             , tt1.sku_num_sum
             , tt2.category_id
             , tt2.category_name
        FROM tmp1 tt1
            LEFT JOIN tmp2 tt2 ON tt1.sku_id = tt2.sku_id
    )
    , tmp4 AS (
        -- step4. 使用开窗函数
        SELECT
            *
             -- 聚合开窗函数，计算每个品类商品个数
             , count(sku_id) OVER (PARTITION BY category_id) AS category_sku_count
             -- 排序开窗函数，加序号：品类分组，销量排序
             , rank() over (PARTITION BY category_id ORDER BY sku_num_sum DESC) AS rk
        FROM tmp3
    )
SELECT
    category_id, category_name
    , category_sku_count
    , sku_id, sku_name
    , sku_num_sum
FROM tmp4
WHERE rk = 1
;



-- todo: 4）、查询用户的累计消费金额及VIP等级
/*
从订单信息表(order_info)中统计每个用户截止其每个下单日期的累积消费金额，以及每个用户在其每个下单日期的VIP等级。
	用户vip等级根据累积消费金额计算，计算规则如下：设累积消费总额为X，
若0=<X<10000,则vip等级为普通会员
若10000<=X<30000,则vip等级为青铜会员
若30000<=X<50000,则vip等级为白银会员
若50000<=X<80000,则vip为黄金会员
若80000<=X<100000,则vip等级为白金会员
若X>=100000,则vip等级为钻石会员
*/
/*
    todo SQL函数中，2个判断函数:
        第1、if函数
            if(condition, true-value, false-value)
            单个条件判断
        第2、when函数
            多个条件判断
            case
                when condition1 then value1
                when condition2 then value2
                when condition3 then value3
                ...
                else value-false
            end
        SQL中聚合开窗函数，在聚合函数之上加上窗口，然后聚合计算
            sum、count、max、min、avg
            自定义udaf函数 = User Definition Aggregation Function
*/
WITH
    tmp1 AS (
        -- step1. 用户分组，统计消费金额
        SELECT
            user_id
            , create_date
             , sum(total_amount) AS total_amount_sum_day
        FROM hive_sql_zg6.order_info
        GROUP BY user_id, create_date
    )
    , tmp2 AS (
        -- step2. 聚合开窗函数，每个用户分组，日期排序，累计金额
        SELECT
            user_id, create_date, total_amount_sum_day
            , sum(total_amount_sum_day) OVER (PARTITION BY user_id ORDER BY create_date) AS total_amount_sum
        FROM tmp1
    )
-- step3. 按照规则，进行匹配，确定VIP登记
SELECT
    user_id
    , create_date
    , total_amount_sum_day
    , total_amount_sum
    , CASE
        WHEN total_amount_sum >= 100000 THEN '钻石会员'
        WHEN total_amount_sum >= 80000 THEN '白金会员'
        WHEN total_amount_sum >= 50000 THEN '黄金会员'
        WHEN total_amount_sum >= 30000 THEN '白银会员'
        WHEN total_amount_sum >= 10000 THEN '青铜会员'
        ELSE '普通会员'
    END AS vip_level
FROM tmp2
;



-- todo: 5）、查询首次下单后第二天连续下单的用户比率
/*
从订单信息表(order_info)中查询首次下单后第二天仍然下单的用户占所有下单用户的比例，结果保留一位小数，使用百分数显示。
*/
/*
    todo 分析思路：
        step1. 去重
        step2. 加序号
        step3. 首次下单日期和第二次下单日期
        step4. 计数
*/
WITH
-- step1. 去重：同一天下单多次
    tmp1 AS (
        SELECT
            user_id, create_date
        FROM hive_sql_zg6.order_info
        GROUP BY user_id, create_date
    )
-- step2. 加序号：用户ID分区，订单日期排序
    , tmp2 AS (
        SELECT
            user_id, create_date
             , row_number() OVER (PARTITION BY user_id ORDER BY create_date) AS rnk
        FROM tmp1
    )
-- step3. 过滤：获取每个用户前2次下单日期
    , tmp3 AS (
        SELECT
            user_id
             -- 首日下单日期
             , min(create_date) AS first_order_date
             -- 除首日之外，再次下单日期
             , max(create_date) AS second_order_date
        FROM tmp2
        WHERE rnk <= 2
        GROUP BY user_id
    )
-- step4. 计数
SELECT
    -- 下单人数
    count(user_id) AS user_count_first
    -- 次日下单人数
    , sum(if(date_add(first_order_date, 1) = second_order_date, 1, 0)) AS user_count_second
    -- 计算占比
    , concat(
        round(
            sum(if(date_add(first_order_date, 1) = second_order_date, 1, 0)) / count(user_id) * 100,
            1
        ),
        '%'
    ) AS second_rate
FROM tmp3
;


