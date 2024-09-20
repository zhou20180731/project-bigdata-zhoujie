
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_senior ;
USE hive_senior;

-- 1)建表语句
DROP TABLE IF EXISTS hive_senior.live_events;
CREATE TABLE IF NOT EXISTS hive_senior.live_events
(
    user_id      INT COMMENT '用户id',
    live_id      INT COMMENT '直播id',
    in_datetime  STRING COMMENT '进入直播间时间',
    out_datetime STRING COMMENT '离开直播间时间'
)
COMMENT '直播间访问记录';

-- 2）数据装载
INSERT OVERWRITE TABLE hive_senior.live_events
VALUES (100, 1, '2021-12-01 19:00:00', '2021-12-01 19:28:00'),
       (100, 1, '2021-12-01 19:30:00', '2021-12-01 19:53:00'),
       (100, 2, '2021-12-01 21:01:00', '2021-12-01 22:00:00'),
       (101, 1, '2021-12-01 19:05:00', '2021-12-01 20:55:00'),
       (101, 2, '2021-12-01 21:05:00', '2021-12-01 21:58:00'),
       (102, 1, '2021-12-01 19:10:00', '2021-12-01 19:25:00'),
       (102, 2, '2021-12-01 19:55:00', '2021-12-01 21:00:00'),
       (102, 3, '2021-12-01 21:05:00', '2021-12-01 22:05:00'),
       (104, 1, '2021-12-01 19:00:00', '2021-12-01 20:59:00'),
       (104, 2, '2021-12-01 21:57:00', '2021-12-01 22:56:00'),
       (105, 2, '2021-12-01 19:10:00', '2021-12-01 19:18:00'),
       (106, 3, '2021-12-01 19:01:00', '2021-12-01 21:10:00');
-- 测试
SELECT * FROM hive_senior.live_events LIMIT 10 ;

-- todo 统计各直播间最大同时在线人数，期望结果如下：
/*
live_id	max_user_count
1	4
2	3
3	2
 */









