
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、各品类中商品价格的中位数
/*
从商品信息表（sku_info）中，统计每给分类中商品价格的中位数，
    如果某分类中商品个数为偶数，则输出中间两个价格的平均值，
    如果是奇数，则输出中间价格即可。
*/
WITH
-- step1. 加序号和整个品类数目
    tmp1 AS (
        SELECT
            sku_id, name, category_id, from_date, price
             , row_number() over (partition by category_id order by price) AS rnk
             , count(*) over (partition by category_id) AS category_cnt
        FROM hive_sql_zg6.sku_info
    )
-- step2. 奇数数据
    , tmp2 AS (
        SELECT
            category_id, collect_list(price)
             , sum(
                if(ceil(category_cnt / 2) == rnk, price, 0)
            ) AS middle_price
        FROM tmp1
        WHERE category_cnt % 2 == 1
        GROUP BY category_id
    )
-- step3. 偶数数据
   , tmp3 AS (
        SELECT
            category_id, collect_list(price)
             , sum(
                if((category_cnt / 2 == rnk) OR (category_cnt / 2 == rnk - 1), price, 0)
            ) / 2 AS middle_price
        FROM tmp1
        WHERE category_cnt % 2 == 0
        GROUP BY category_id
   )
-- step4. 合并数据
SELECT * FROM  tmp2
UNION
SELECT * FROM tmp3
;




-- todo: 2）、找出销售额连续3天超过100的商品
/*
从订单详情表（order_detail）中找出销售额连续3天超过100的商品。
*/
WITH
-- step1. 每个商品每天销售额
tmp1 AS
    (
        SELECT
            sku_id
             , create_date
             , sum(price * sku_num) AS sku_price_sum
        FROM hive_sql_zg6.order_detail
        GROUP BY sku_id, create_date
        HAVING sum(price * sku_num) > 100
    ),
-- step2. 每行数据加上编号
tmp2 AS
    (
        SELECT
            sku_id, create_date, sku_price_sum
             , row_number() OVER (PARTITION BY sku_id ORDER BY create_date) AS rk
        FROM tmp1
    ),
-- step3. 计算差值
tmp3 AS
    (
        SELECT
            sku_id, create_date, sku_price_sum, rk
             , date_sub(create_date, rk) AS date_diff
        FROM tmp2
    ),
-- step4. 商品ID和日期差值 分组
tmp4 AS
    (
        SELECT
            sku_id
             , date_diff
             , count(*) AS days
             , collect_list(create_date) AS date_list
        FROM tmp3
        GROUP BY sku_id, date_diff
    )
-- step5. 过滤及去重
SELECT
    DISTINCT sku_id
FROM tmp4
WHERE days >= 3
;



-- todo: 3）、查询有新注册用户的当天的新用户数量、新用户的第一天留存率
/*
从用户登录明细表（user_login_detail）中首次登录算作当天新增，第二天也登录了算作一日留存。
*/
WITH
    tmp1 AS (
        -- step1. 找到每个用户最小登录日期（也就是注册日期）
        SELECT
            user_id
             , date_format(min(login_ts), 'yyyy-MM-dd') AS register_date
        FROM hive_sql_zg6.user_login_detail
        GROUP BY user_id
    )
    , tmp2 AS (
        -- step2. 关联登录日志表，获取注册用户第2天登录用户
        SELECT
            t1.user_id, t1.register_date
             , t2.user_id AS next_user_id
             , date_format(t2.login_ts, 'yyyy-MM-dd') AS next_login_date
        FROM tmp1 t1
             LEFT JOIN hive_sql_zg6.user_login_detail t2
                ON t1.user_id = t2.user_id
                   AND date_add(t1.register_date, 1) = date_format(t2.login_ts, 'yyyy-MM-dd')
    )
-- step3. 注册日期分组，统计每日新增用户数和1日留存用户数
SELECT
    register_date
    , count(DISTINCT user_id) AS register_user_count
    , count(DISTINCT next_user_id) AS login_user_count_1d
    , cast(count(DISTINCT next_user_id) / count(DISTINCT user_id) AS DECIMAL(16, 2)) AS rate_login_1d
FROM tmp2
GROUP BY register_date
;

-- todo: 4）、求出商品连续售卖的时间区间
/*
从订单详情表（order_detail）中，求出商品连续售卖的时间区间。
*/



-- todo: 5）、登录次数及交易次数统计
/*
分别从登陆明细表（user_login_detail）和配送信息表中用户登录时间和下单时间统计登陆次数和交易次数
*/






