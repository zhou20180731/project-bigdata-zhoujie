
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_sql_zg5;
-- 使用数据库
USE hive_sql_zg5 ;

-- todo: 3.1、查询结果排序&分组指定条件
-- 1）、查询学生的总成绩并按照总成绩降序排序；

SELECT
    stu_id
     , sum(score) AS score_total
FROM hive_sql_zg5.score_info
GROUP BY stu_id
ORDER BY score_total DESC
;

-- 2）、按如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示；
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩
-- 001    98   80  60    3       83
-- 002    85   90  0     2       87.5

WITH
    tmp1 AS (
        -- step1. 关联课程，获取名称
        SELECT
            t1.stu_id, t1.course_id, t1.score, t2.course_name
        FROM hive_sql_zg5.score_info t1
                 LEFT JOIN hive_sql_zg5.course_info t2 ON t1.course_id = t2.course_id
    )
-- step2. 按照学生分组
SELECT
    stu_id AS `学生id`
     , sum(if(course_name = '语文', score, 0)) AS `语文`
     , sum(if(course_name = '数学', score, 0)) AS `数学`
     , sum(if(course_name = '英语', score, 0)) AS `英语`
     , count(course_id) AS `有效课程数`
     , round(avg(score), 2) AS `有效平均成绩`
FROM tmp1
GROUP BY stu_id
;


/*
todo 如何进行列转行
学生id 语文 数学 英语
011,61.00,49.00,70.00
012,44.00,74.00,62.00
013,47.00,35.00,93.00
014,81.00,39.00,32.00
015,90.00,48.00,84.00
016,71.00,89.00,71.00
017,58.00,34.00,55.00
018,38.00,58.00,49.00
019,46.00,39.00,93.00
020,89.00,59.00,81.00
|
001,01,94.00,语文
001,02,63.00,数学
001,03,79.00,英语
001,04,54.00,体育
002,01,74.00,语文
002,02,84.00,数学
002,03,87.00,英语
002,04,100.00,体育
......
*/


DESC FUNCTION concat;

-- 3）、查询一共参加三门课程且其中一门为语文课程的学生的id和姓名；
SELECT course_id FROM hive_sql_zg5.course_info WHERE course_name = '语文' ;

WITH
    tmp1 AS (
        -- step2. 找到参加3门课程学生
        SELECT
            stu_id
             , count(distinct course_id) AS course_cnt
             , collect_set(course_id) AS course_list
        FROM hive_sql_zg5.score_info
        GROUP BY stu_id
        HAVING course_cnt = 3
           AND array_contains(course_list, '01')
    )
SELECT
    t1.stu_id, t2.stu_name
FROM tmp1 t1
         LEFT JOIN hive_sql_zg5.student_info t2 ON t1.stu_id = t2.stu_id
;

DESC FUNCTION array_contains ;

-- todo：3.2、子查询
-- 1）、查询所有课程成绩均小于60分的学生的学号、姓名；
WITH
    tmp1 AS (
        -- step1. 获取学号：每个学生最高分成绩如果小于60，表示所有科目成绩小于60
        SELECT
            stu_id
        -- , collect_list(score) AS score_list
        FROM hive_sql_zg5.score_info
        GROUP BY stu_id
        HAVING max(score) < 60
    )
-- step2. 关联学生信息，获取学生姓名
SELECT
    t1.stu_id, t2.stu_name
FROM tmp1 t1
         LEFT JOIN hive_sql_zg5.student_info t2 ON t1.stu_id = t2.stu_id
;




-- 2）、查询没有学全所有课的学生的学号、姓名；
- step1. 所有科目总数
SELECT count(course_id) AS cnt FROM hive_sql_zg5.course_info;

WITH
    tmp1 AS (
        -- step1. 学生所学课程的数目
        SELECT
            stu_id
             , count(DISTINCT course_id) AS stu_cnt
        FROM hive_sql_zg5.score_info
        GROUP BY stu_id
    )
   , tmp2 AS (
    SELECT
        stu_id
         , stu_cnt
    FROM tmp1
    WHERE stu_cnt < (
        -- step2. 所有科目总数
        SELECT count(course_id) AS cnt FROM hive_sql_zg5.course_info
    )
)
-- step3. 关联学生信息，获取学生姓名
SELECT
    t2.stu_id, t3.stu_name
FROM tmp2 t2
         LEFT JOIN hive_sql_zg5.student_info t3 ON t2.stu_id = t3.stu_id
;


-- 3）、查询出只选修了三门课程的全部学生的学号和姓名；

WITH
    tmp1 AS (
        -- step1. 学生所学课程的数目
        SELECT
            stu_id
             , count(DISTINCT course_id) AS stu_cnt
        FROM hive_sql_zg5.score_info
        GROUP BY stu_id
        HAVING stu_cnt = 3
    )
-- step2. 关联学生信息，获取学生姓名
SELECT
    t1.stu_id, t2.stu_name
FROM tmp1 t1
         LEFT JOIN hive_sql_zg5.student_info t2 ON t1.stu_id = t2.stu_id
;


