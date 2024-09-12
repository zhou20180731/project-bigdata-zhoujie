
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_sql_zg5;
-- 使用数据库
USE hive_sql_zg5 ;

-- todo: 2.1、分组
-- 1）、查询各科成绩最高和最低的分，以如下的形式显示：课程号，最高分，最低分；
 SELECT  course_id
      ,max(score) AS max_score
      ,min(score) As min_score
 FROM
    hive_sql_zg5.score_info
 GROUP BY course_id;

-- 2）、查询每门课程有多少学生参加了考试（有考试成绩）；
--todo ；两种-正常方法 优化方法

 SELECT
         course_id
       ,collect_list(stu_id)
    ,count(stu_id) AS cnt_stu
 FROM  hive_sql_zg5.score_info
 GROUP BY  course_id;


-- 3）、查询男生、女生人数；
SELECT sex, COUNT(*) AS total
FROM hive_sql_zg5.student_info
GROUP BY sex;

-- todo：2.2、分组结果的条件
-- 1）、查询平均成绩大于60分的学生的学号和平均成绩；  分组聚合

SELECT
    stu_id
     , AVG(score) AS avg_score
   FROM hive_sql_zg5. score_info
   GROUP BY stu_id
  HAVING AVG(score) > 60;

-- 2）、查询至少选修四门课程的学生学号；
SELECT  stu_id
        ,count( DISTINCT course_id) AS cnt_course
FROM hive_sql_zg5.score_info
group by stu_id
having  cnt_course >4;

-- 3）、查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓；
SELECT  substr( stu_name,1,1) As sub_name
       ,count(*)
FROM
hive_sql_zg5.student_info
GROUP BY substr(stu_name,1,1)
HAVING  count(*)>2 ;


-- 4）、查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列；
SELECT
    course_id
     ,avg(score) As avg_score
FRoM hive_sql_zg5. score_info
GROUP BY course_id
ORDER BY  avg_score,course_id desc ;


-- 5）、统计参加考试人数大于等于15的学科；
SELECT  course_id
,count(stu_id) As cnt_stuid
FROM score_info
GROUP BY course_id
having  count(stu_id)>15;
