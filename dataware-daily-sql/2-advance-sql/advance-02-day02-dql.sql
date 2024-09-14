
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、每个商品销售首年的年份、销售数量和销售金额
/*
从订单明细表(order_detail)统计每个商品销售首年的年份，销售数量和销售总额。
*/
/*
    todo 思路分析:
        step1. 按照商品、年份分组，聚合统计数量和金额
        step2. 加序号：商品分组，年份排序
        step3. 过滤
*/
WITH
-- step1. 按照商品、年份分组，聚合统计数量和金额
    tmp1 AS (
        SELECT
            sku_id
             , date_format(create_date, 'yyyy') AS sale_year
             , sum(sku_num) AS sku_num_year
             , sum(sku_num * price) AS sku_amount_year
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id, date_format(create_date, 'yyyy')
    )
-- step2. 加序号：商品分组，年份排序
    , tmp2 AS (
        SELECT
            *, row_number() OVER (PARTITION BY sku_id ORDER BY sale_year) AS rnk
        FROM tmp1
    )
-- step3. 过滤
SELECT
    sku_id, sale_year, sku_num_year, sku_amount_year
FROM tmp2
WHERE rnk = 1;

-- todo 日期函数
/*
    date_format(date, 'yyyy-MM-dd HH:mm:ss.SSS')
    year
    month
    day
*/
SELECT
    date_format('2024-06-29 11:54:38.999', 'yyyy-MM-dd') AS x1
    , date_format('2024-06-29 11:54:38.999', 'yyyy') AS x11
    , date_format('2024-06-29 11:54:38.999', 'MM') AS x12
    , date_format('2024-06-29 11:54:38.999', 'dd') AS x13
    , year('2024-06-29 11:54:38.999') AS x2
    , month('2024-06-29 11:54:38.999') AS x3
    , day('2024-06-29 11:54:38.999') AS x4
    , substring('2024-06-29 11:54:38.999', 1, 4) AS x22
    , CAST(substring('2024-06-29 11:54:38.999', 6, 2) AS INT) AS x32
    , CAST(substring('2024-06-29 11:54:38.999', 9, 2) AS INT) AS x42
;


-- todo: 2）、筛选去年总销量小于100的商品
/*
从订单明细表(order_detail)中筛选出去年总销量小于100的商品及其销量，
	假设今天的日期是2022-01-10，不考虑上架时间小于一个月的商品
*/
/*
    分析思路：
        1. 去年交易数据
        2. 商品上架时间大于一个月
        【先过滤where、再分组group by、最后聚合aggregation】
    todo 函数：add_months、concat
*/
SELECT
    sku_id, sum(sku_num) AS sku_num_year
FROM hive_sql_zg6.order_detail
WHERE year(create_date) = add_months('2022-01-10', -12, 'yyyy')
    AND sku_id IN (
        SELECT sku_id
        FROM hive_sql_zg6.sku_info
        WHERE from_date < concat(add_months('2022-01-10', -12, 'yyyy'), '-12-01')
    )
GROUP BY sku_id
HAVING sum(sku_num) < 100
;

-- 函数add_months
DESC FUNCTION add_months;
/*
add_months(start_date, num_months, output_date_format)
    - Returns the date that is num_months after start_date.
*/
SELECT
    add_months('2022-01-10', 1, 'yyyy-MM-dd') AS x1
    , add_months('2022-01-10', -1, 'yyyy-MM-dd') AS x2
    , add_months('2022-01-10', -12, 'yyyy') AS x3
;

-- todo: 3）、查询每日新用户数
/*
从用户登录明细表（user_login_detail）中查询每天的新增用户数，
若一个用户在某天登录了，且在这一天之前没登录过，则认为该用户为这一天的新增用户。
*/
/*
    思路分析：
        找到每个用户最小登录时间，就是用户新增时间，统计个数即可
*/
WITH
-- step1. 找到每个用户注册日期
    tmp1 AS (
        SELECT
            user_id
             , date_format(min(login_ts), 'yyyy-MM-dd') AS register_date
        FROM hive_sql_zg6.user_login_detail
        GROUP BY user_id
    )
-- step2. 按照注册日期分组统计，新增用户数
SELECT
    register_date
    , count(user_id) AS new_user_count
FROM tmp1
GROUP BY register_date
;


-- todo: 4）、统计每个商品的销量最高的日期
/*
从订单明细表（order_detail）中统计出每种商品销售件数最多的日期及当日销量，
如果有同一商品多日销量并列的情况，取其中的最小日期。
*/
WITH
-- step1. 计算每个商品每日的总销量
    tmp1 AS (
        SELECT
            sku_id
             , create_date
             , sum(sku_num) AS sku_num_day
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id, create_date
    )
-- step2. 加序号：商品分区，先销量排序，后日期排序
    , tmp2 AS (
        SELECT
            sku_id
             , create_date
             , sku_num_day
             , row_number() OVER (PARTITION BY sku_id ORDER BY sku_num_day DESC, create_date) AS rk
        FROM tmp1
    )
-- step3. 过滤获取最高
SELECT
    sku_id, create_date, sku_num_day
FROM tmp2
WHERE rk = 1
;


-- todo: 5）、查询销售件数高于品类平均数的商品
/*
从订单明细表（order_detail）中查询累积销售件数高于其所属品类平均数的商品
*/
WITH
-- step1. 商品统计销量
    tmp1 AS (
        SELECT
            sku_id
            , sum(sku_num) AS sku_num_sum
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id
    )
-- step2. 关联品类表
    , tmp2 AS (
        SELECT
            t2.category_id
            , t1.sku_id
            , t1.sku_num_sum
        FROM tmp1 t1
        LEFT JOIN hive_sql_zg6.sku_info t2 ON t1.sku_id = t2.sku_id
    )
-- step3. 开窗聚合函数，计算每个品类平均销量
    , tmp3 AS (
        SELECT
            category_id, sku_id, sku_num_sum
            , avg(sku_num_sum) OVER (PARTITION BY category_id) AS category_avg_num
        FROM tmp2
    )
-- step4. 过滤：商品销量 大于 品类平均
SELECT
    category_id, sku_id, sku_num_sum, category_avg_num
FROM tmp3
WHERE sku_num_sum > category_avg_num
;