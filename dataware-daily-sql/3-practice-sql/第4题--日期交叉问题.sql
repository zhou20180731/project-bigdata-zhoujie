
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_senior ;
USE hive_senior;


-- 1)建表语句
DROP TABLE IF EXISTS hive_senior.promotion_info;
CREATE TABLE hive_senior.promotion_info
(
    promotion_id STRING COMMENT '优惠活动id',
    brand        STRING COMMENT '优惠品牌',
    start_date   STRING COMMENT '优惠活动开始日期',
    end_date     STRING COMMENT '优惠活动结束日期'
) COMMENT '各品牌活动周期表';

-- 2）数据装载
INSERT OVERWRITE TABLE hive_senior.promotion_info
VALUES (1, 'oppo', '2021-06-05', '2021-06-09'),
       (2, 'oppo', '2021-06-11', '2021-06-21'),
       (3, 'vivo', '2021-06-05', '2021-06-15'),
       (4, 'vivo', '2021-06-09', '2021-06-21'),
       (5, 'redmi', '2021-06-05', '2021-06-21'),
       (6, 'redmi', '2021-06-09', '2021-06-15'),
       (7, 'redmi', '2021-06-17', '2021-06-26'),
       (8, 'huawei', '2021-06-05', '2021-06-26'),
       (9, 'huawei', '2021-06-09', '2021-06-15'),
       (10, 'huawei', '2021-06-17', '2021-06-21');

-- 测试
SELECT * FROM hive_senior.promotion_info LIMIT 20 ;

-- 验证，插入数据
INSERT INTO TABLE hive_senior.promotion_info
	VALUES (11, 'vivo', '2021-06-15', '2021-06-22'),
	       (12, 'redmi', '2021-06-21', '2021-06-27') ;


-- todo: 统计每个品牌的优惠总天数，若某个品牌在同一天有多个优惠活动，则只按一天计算。期望结果如下：
/*
brand	promotion_day_count
vivo	17
oppo	16
redmi	22
huawei	22
*/









