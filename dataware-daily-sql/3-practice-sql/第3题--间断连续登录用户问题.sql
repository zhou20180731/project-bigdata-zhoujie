
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_senior ;
USE hive_senior;

-- 1)建表语句
DROP TABLE IF EXISTS hive_senior.login_events;
CREATE TABLE IF NOT EXISTS hive_senior.login_events
(
    user_id        INT COMMENT '用户id',
    login_datetime STRING COMMENT '登录时间'
)
COMMENT '直播间访问记录';

-- 2）数据装载
INSERT OVERWRITE TABLE hive_senior.login_events
VALUES (100, '2021-12-01 19:00:00'),
       (100, '2021-12-01 19:30:00'),
       (100, '2021-12-02 21:01:00'),
       (100, '2021-12-03 11:01:00'),
       (101, '2021-12-01 19:05:00'),
       (101, '2021-12-01 21:05:00'),
       (101, '2021-12-03 21:05:00'),
       (101, '2021-12-05 15:05:00'),
       (101, '2021-12-06 19:05:00'),
       (102, '2021-12-01 19:55:00'),
       (102, '2021-12-01 21:05:00'),
       (102, '2021-12-02 21:57:00'),
       (102, '2021-12-03 19:10:00'),
       (104, '2021-12-04 21:57:00'),
       (104, '2021-12-02 22:57:00'),
       (105, '2021-12-01 10:01:00');

-- 额外添加数据
INSERT INTO TABLE hive_senior.login_events
VALUES (101, '2021-12-11 19:00:00'),
       (101, '2021-12-12 19:30:00'),
       (101, '2021-12-14 21:01:00') ;

-- 测试
SELECT * FROM hive_senior.login_events ;


-- todo 统计各用户最长的连续登录天数，间断一天也算作连续，例如：一个用户在1,3,5,6登录，则视为连续6天登录。
/*
user_id	max_day_count
100	3
101	6
102	3
104	3
105	1
*/




