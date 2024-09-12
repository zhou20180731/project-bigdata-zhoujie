
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_sql_zg5;
-- 使用数据库
USE hive_sql_zg5 ;

/*
数据分析中，数据调研：数据有哪些（表有哪些，如何产生的数据，核心数据：业务过程）
    course_info 课程表
    student_info 学生表
    teacher_info 教师表
        维度表，环境
    score_info  成绩表
        事实表，学生学习某个课程，并且考试产生的数据
		分析数据，往往分析就是事实表的数据，此处就是score_info表
*/


-- todo: 1.1、简单查询
-- 1）、检索课程编号为“04”且分数小于60的学生的课程信息，结果按分数降序排列；
SELECT
      stu_id
      , course_id
      , score
FROM hive_sql_zg5.score_info
WHERE course_id='04' AND score<60
ORDER BY  score DESC ;

-- 2）、查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序；
-- 2.1 查询数学成绩的编号
SELECT course_id
FROM hive_sql_zg5.course_info
WHERE course_name ='数学'
;

--2.2
SELECT * FROM hive_sql_zg5. score_info
WHERE  course_id=(SELECT course_id
                  FROM hive_sql_zg5.course_info
                  WHERE course_name ='数学')

  AND  score<60
ORDER BY stu_id desc ;

-- 3）、查询姓名中带“冰”的学生名单；
SELECT stu_id
     , stu_name
     , birthday
     , sex
    from student_info
    WHERE  stu_name LIKE '%冰%';
-- 4）、查询姓“王”老师的个数；

select  count(tea_id) as cnt
From hive_sql_zg5.teacher_info
where tea_name like  '王%';


-- todo：1.2、汇总分析
/*
	SQL分析中，5个基本聚合函数使用
		count 计数
		sum 求和
		avg 均值
		max 最大
		min 最小
*/
-- 1）、查询编号为“02”的课程的总成绩；
-- SELECT
--     '02' AS score_name
-- from  hive_sql_zg5.score_info
-- Where course_id ='02';


SELECT
    '02' AS score_name
     , sum(score) AS sum_score
     , count(stu_id) AS cnt_stu
     , max(score) AS max_score
     , min(score) AS min_score
     , round(avg(score), 2) AS avg_score
     , cast(avg(score) AS DECIMAL(16, 2)) AS avg_score
     , avg(score) AS avg_score
     , collect_list(score) AS score_list
FROM hive_sql_zg5.score_info
WHERE course_id = '02'
;


-- 2）、查询参加考试的学生个数；

-- todo 当数据量很大时，上述SQL容易产生数据倾斜，因为使用count distinct只有1个Reduce任务，采用group by + count优化
/*
    案例剖析：https://blog.51cto.com/u_16099335/6687050
*/

SELECT
    count(DISTINCT stu_id) AS stu_cnt
FROM hive_sql_zg5.score_info
;


SELECT
    count(stu_id) AS stu_cnt
FROM (
--           group by 去重
         SELECT
             stu_id
         FROM hive_sql_zg5.score_info
         GROUP BY stu_id
     ) t1

;
/**
-- 查询日志
[bwie@node101 ~]$ hv.sh beeline
    执行SQL语句，查看运行日志

-- 查询计划
explain SELECT ....
*/

