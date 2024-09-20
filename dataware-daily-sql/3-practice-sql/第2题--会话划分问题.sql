
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_senior ;
USE hive_senior;

-- 1)建表语句
DROP TABLE IF EXISTS hive_senior.page_view_events;
CREATE TABLE IF NOT EXISTS hive_senior.page_view_events
(
    user_id        INT COMMENT '用户id',
    page_id        STRING COMMENT '页面id',
    view_timestamp BIGINT COMMENT '访问时间戳'
)
COMMENT '页面访问记录';

-- 2）数据装载
INSERT OVERWRITE TABLE hive_senior.page_view_events
VALUES (100, 'home', 1659950435),
       (100, 'good_search', 1659950446),
       (100, 'good_list', 1659950457),
       (100, 'home', 1659950541),
       (100, 'good_detail', 1659950552),
       (100, 'cart', 1659950563),
       (101, 'home', 1659950435),
       (101, 'good_search', 1659950446),
       (101, 'good_list', 1659950457),
       (101, 'home', 1659950541),
       (101, 'good_detail', 1659950552),
       (101, 'cart', 1659950563),
       (102, 'home', 1659950435),
       (102, 'good_search', 1659950446),
       (102, 'good_list', 1659950457),
       (103, 'home', 1659950541),
       (103, 'good_detail', 1659950552),
       (103, 'cart', 1659950563);

-- 测试
SELECT * FROM hive_senior.page_view_events LIMIT 10 ;


-- todo 题目需求：属于同一会话的访问记录增加一个相同的会话id字段
/*
	规定若同一用户的相邻两次访问记录时间间隔小于60s，则认为两次浏览记录属于同一会话。
user_id	page_id	    view_timestamp	session_id
100	    home	    1659950435	    100-1
100	    good_search	1659950446	    100-1
100	    good_list	1659950457	    100-1
100	    home	    1659950541	    100-2
100	    good_detail	1659950552	    100-2
100	    cart	    1659950563	    100-2
101	    home	    1659950435	    101-1
101	    good_search	1659950446	    101-1
101	    good_list	1659950457	    101-1
*/





