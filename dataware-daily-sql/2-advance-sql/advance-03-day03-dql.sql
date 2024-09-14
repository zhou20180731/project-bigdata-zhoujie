
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、用户注册、登录、下单综合统计
/*
从用户登录明细表（user_login_detail）和订单信息表（order_info）中
查询每个用户的注册日期（首次登录日期）、总登录次数以及其在2021年的登录次数、订单数和订单总额。
*/
/*
    思路分析：
        1. log表，统计次数和注册日期
        2. order表，统计订单数和订单总额
        3. 关联join
*/
WITH
-- step1. log表，统计次数和注册日期
    tmp1 AS (
        SELECT
            user_id
             , date_format(min(login_ts), 'yyyy-MM-dd') AS register_date
             , count(user_id) AS login_count
             , sum(
                if(date_format(login_ts, 'yyyy')= '2021', 1, 0)
            ) AS login_count_2021
        FROM hive_sql_zg6.user_login_detail
        GROUP BY user_id
    )
-- 2. order表，统计订单数和订单总额
    , tmp2 AS (
        SELECT
            user_id
            , count(order_id) AS order_count_2021
            , sum(total_amount) AS order_amount_2021
        FROM hive_sql_zg6.order_info
        WHERE date_format(create_date, 'yyyy') = '2021'
        GROUP BY user_id
    )
-- 3. 关联join
SELECT
    t1.user_id, register_date, login_count, login_count_2021
    , t2.order_count_2021, t2.order_amount_2021
FROM tmp1 t1
LEFT JOIN tmp2 t2 ON t1.user_id = t2.user_id
;


-- todo: 2）、查询指定日期的全部商品价格
/*
从商品价格修改明细表（sku_price_modify_detail）中查询2021-10-01的全部商品的价格，假设所有商品初始价格默认都是99。
*/
WITH
-- step1. 获取价格修改表中截止2021-10-01数据
    tmp1 AS (
        SELECT
            sku_id
             , new_price
             , change_date
             , row_number() over (PARTITION BY sku_id ORDER BY change_date DESC) AS rnk
        FROM hive_sql_zg6.sku_price_modify_detail
        WHERE change_date <= '2021-10-01'
    )
-- step2. 商品修改后最新价格
    , tmp2 AS (
        SELECT
            sku_id
             , new_price
        FROM tmp1
        WHERE rnk = 1
    )
-- step3. 商品表关联
SELECT
    t1.sku_id
    , if(t2.new_price IS NOT NULL , t2.new_price, 99.0) AS sku_price
FROM hive_sql_zg6.sku_info t1
    LEFT JOIN tmp2 t2 ON t1.sku_id = t2.sku_id
;



-- todo: 3）、即时订单比例
/*
订单配送中，如果期望配送日期和下单日期相同，称为即时订单，
如果期望配送日期和下单日期不同，称为计划订单。
请从配送信息表（delivery_info）中求出每个用户的首单（用户的第一个订单）中即时订单的比例，保留两位小数，以小数形式显示。
*/
WITH
-- step1. 加序号：用户分区，订单日期升序
    tmp1 AS (
        SELECT
            user_id
             , order_date
             , custom_date
             , row_number() over (partition by user_id ORDER BY order_date) AS rnk
        FROM hive_sql_zg6.delivery_info
    )
-- step2. 获取用户首单
    , tmp2 AS (
        SELECT
            user_id, order_date, custom_date
        FROM tmp1
        WHERE rnk = 1
    )
-- step3. 计算及时订单数目、计划订单数目和及时订单占比
SELECT
    -- 总订单数
    count(user_id) AS order_count
    -- 及时订单数
    , sum(if(order_date = custom_date, 1, 0)) AS realtime_order_count
    -- 计划订单数
    , sum(if(order_date != custom_date, 1, 0)) AS plan_order_count
    -- 及时订单占比
    , cast(
        sum(if(order_date = custom_date, 1, 0)) / count(user_id) AS DECIMAL(16, 2)
    ) AS realtime_order_rate
FROM tmp2
;


-- todo: 4）、向用户推荐朋友收藏的商品
/*
现需要请向所有用户推荐其朋友收藏但是用户自己未收藏的商品，
请从好友关系表（friendship_info）和收藏表（favor_info）中查询出应向哪位用户推荐哪些商品。
*/
WITH
-- step1. 找朋友：好友是双向的
    tmp1 AS (
        SELECT user1_id AS user_id, user2_id AS friendship_id
        FROM hive_sql_zg6.friendship_info WHERE user1_id != user2_id
        UNION
        SELECT user2_id AS user_id, user1_id AS friendship_id
        FROM hive_sql_zg6.friendship_info WHERE user1_id != user2_id
    )
-- step2. 关联收藏表
    , tmp2 AS (
        SELECT
            t1.user_id
             , t1.friendship_id
             -- 推荐收藏商品
             , t2.sku_id
        FROM tmp1 t1
                 LEFT JOIN hive_sql_zg6.favor_info t2 ON t1.user_id = t2.user_id
    )
-- step3. 关联收藏表
    , tmp3 AS (
        SELECT
            tt1.friendship_id AS user_id
             -- 推荐收藏商品
             , tt1.sku_id
             -- 自己收藏
             , tt2.sku_id AS self_favor_sku_id
        FROM tmp2 tt1
                 LEFT JOIN hive_sql_zg6.favor_info tt2
                           ON tt1.friendship_id = tt2.user_id AND tt1.sku_id = tt2.sku_id
    )
-- step4. 过滤没有收藏商品，作为用户推荐商品
SELECT
    user_id
    , collect_set(sku_id) AS favor_sku_list
FROM tmp3
WHERE self_favor_sku_id IS NULL
GROUP BY user_id
;



-- todo: 5）、查询所有用户的连续登录两天及以上的日期区间
/*
从登录明细表（user_login_detail）中查询出，所有用户的连续登录两天及以上的日期区间，以登录时间（login_ts）为准。
*/
/*
    思路分析：
        step1. 去重
        step2. 加序号
        step3. 日期差值
        step4. 分组、过滤、聚合
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
-- step2. 加序号
    , tmp2 AS (
        SELECT
            user_id
            , login_date
            , row_number() over (PARTITION BY user_id ORDER BY login_date) AS rnk
        FROM tmp1
    )
-- step3. 日期差值
    , tmp3 AS (
        SELECT
            user_id, login_date
            , date_sub(login_date, rnk) AS date_diff
        FROM tmp2
    )
-- step4. 分组、过滤、聚合
SELECT
    user_id
--     , date_diff
--     , count(user_id) AS continue_days
--     , collect_list(login_date) AS login_date_list
    , min(login_date) AS start_login_date
    , max(login_date) AS end_login_date
FROM tmp3
GROUP BY user_id, date_diff
HAVING count(user_id) >= 2

