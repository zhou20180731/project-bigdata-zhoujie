
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、查询每个用户登录日期的最大空档期
/*
从登录明细表（user_login_detail）中查询每个用户两个登录日期（以login_ts为准）之间的最大的空档期。
统计最大空档期时，用户最后一次登录至今的空档也要考虑在内，假设今天为2021-10-10。
*/
WITH
-- step1. 去重
    tmp1 AS (
        SELECT
            user_id
             , date_format(login_ts, 'yyyy-MM-dd') AS login_date
        FROM hive_sql_zg6.user_login_detail
        GROUP BY user_id, date_format(login_ts, 'yyyy-MM-dd')
    )
-- step2. lead获取下一天登录日期
    , tmp2 AS (
        SELECT
            user_id, login_date
             , lead(login_date, 1) over (PARTITION BY user_id ORDER BY login_date) AS next_login_date
        FROM tmp1
    )
-- step3. 计算差值
    , tmp3 AS (
        SELECT
            user_id, login_date, datediff(next_login_date, login_date) AS days
        FROM tmp2
    )
-- step4. 计算最大差值
SELECT
    user_id, max(days) AS max_days
FROM tmp3
GROUP BY user_id
;


-- todo: 2）、查询相同时刻多地登陆的用户
/*
从登录明细表（user_login_detail）中查询在相同时刻，多地登陆（ip_address不同）的用户
*/
WITH
-- step1. 下一次数据登录时间和ip地址
    tmp1 AS (
        SELECT
            user_id, login_ts, logout_ts, ip_address
             , lead(login_ts, 1, '9999-12-31') over (PARTITION BY user_id ORDER BY login_ts) AS next_login_ts
             , lead(ip_address, 1, '0.0.0.0') over (PARTITION BY user_id ORDER BY login_ts) AS next_ip_address
        FROM hive_sql_zg6.user_login_detail
    )
-- step2. 比较本次退出时间与下次登录时间，以及两次ip地址不相等
SELECT
    user_id
FROM tmp1
WHERE next_login_ts < logout_ts AND ip_address != next_ip_address
GROUP BY user_id
;



-- todo: 3）、销售额完成任务指标的商品
/*
商家要求每个商品每个月需要售卖出一定的销售总额，假设1号商品销售总额大于21000，2号商品销售总额大于10000，其余商品没有要求。
请写出SQL从订单详情表中（order_detail）查询连续两个月销售总额大于等于任务总额的商品
*/
WITH
-- step1. 先统计每个商品每个月销售总额
    tmp1 AS (
        SELECT
            sku_id
             , trunc(create_date, 'MM') AS sale_month
             , sum(sku_num * price) AS amount_month
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id, trunc(create_date, 'MM')
    )
-- step2. 过滤获取数据
    , tmp2 AS (
        SELECT
            sku_id, sale_month, amount_month
        FROM tmp1
        WHERE (sku_id = '1' AND amount_month > 21000)
            OR (sku_id = '2' AND amount_month > 10000)
            OR (sku_id NOT IN ('1', '2'))
    )
-- step3. 加序号
    , tmp3 AS (
        SELECT
            sku_id, sale_month, amount_month
             , row_number() over (PARTITION BY sku_id ORDER BY sale_month) AS rk
        FROM tmp2
    )
-- step4. 日期差值
    , tmp4 AS (
        SELECT
            sku_id, sale_month, amount_month, rk
             , add_months(sale_month, - rk) AS month_diff
        FROM tmp3
    )
-- step5. 分组、聚合、过滤
SELECT
    sku_id, month_diff
    , count(sku_id) AS continue_month_number
    , collect_list(sale_month) AS sale_month_list
FROM tmp4
GROUP BY sku_id, month_diff
HAVING count(sku_id) >= 2
;



-- todo: 4）、根据商品销售情况进行商品分类
/*
从订单详情表中（order_detail）对销售件数对商品进行分类，
	0-5000为冷门商品，5001-19999位一般商品，20000往上为热门商品，并求出不同类别商品的数量
*/
WITH tmp1 AS (
-- step1. 商品分组，计算销售件数
    SELECT
        sku_id,
        sum(sku_num) AS sku_num_sum
    FROM hive_sql_zg6.order_detail
    GROUP BY sku_id
), tmp2 AS (
-- step2. 按照销售件数对商品进行分类，使用when函数
    SELECT
        sku_id, sku_num_sum,
        CASE
            WHEN sku_num_sum >= 20000 THEN '热门商品'
            WHEN sku_num_sum > 5000 THEN '一般商品'
            ELSE '冷门商品'
        END AS category
    FROM tmp1
)
-- step3. 按照类别category，统计商品数目
SELECT
    category,
    count(sku_id) AS sku_id_count
FROM tmp2
GROUP BY category
;



-- todo: 5）、各品类销量前三的所有商品
/*
从订单详情表中（order_detail）和商品（sku_info）中查询各个品类销售数量前三的商品。如果该品类小于三个商品，则输出所有的商品销量。
*/
WITH tmp1 AS (
-- step1. 商品分组，统计销售数量
    SELECT
        sku_id,
        sum(sku_num) AS sku_num_sum
    FROM hive_sql_zg6.order_detail
    GROUP BY sku_id
), tmp2 AS (
-- step2. 关联商品表，获取category品类id
    SELECT
        t1.sku_id, sku_num_sum, t2.category_id
    FROM tmp1 t1
             LEFT JOIN hive_sql_zg6.sku_info t2 ON t1.sku_id = t2.sku_id
), tmp3 AS (
-- step3. 排序开窗函数加上编号，品类分组销售量排序
    SELECT
        sku_id, sku_num_sum, category_id,
        rank() OVER (PARTITION BY category_id ORDER BY sku_num_sum DESC) AS rnk
    FROM tmp2
)
SELECT
    category_id, sku_id, sku_num_sum
FROM tmp3
WHERE rnk <= 3
;



