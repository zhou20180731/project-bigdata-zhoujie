
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_sql_zg5;
-- 使用数据库
USE hive_sql_zg5 ;

-- todo: 4.1、表连接
-- 1）、查询有两门以上的课程不及格的同学的学号及其平均成绩；
select  stu_id,
        sum(if(score< 60,1,0))As cn_score,
         round(avg(score),2)  As avg_score
from   hive_sql_zg5.score_info
group by  stu_id
HAVING  sum(if(score<60,1,0)) >2;



-- todo 解法2：使用 left join关联，筛选数据
    with tmp1 as (select stu_id
                  from hive_sql_zg5.score_info
                  group by stu_id
                  Having sum(if(score < 60, 1, 0)) < 2
                  )
     SELECT
    t1.stu_id
    , round(avg(t2.score)) AS avg_score
        FROM tmp1 t1
            LEFT JOIN hive_sql_zg5.score_info t2 ON t1.stu_id = t2.stu_id
        GROUP BY t1.stu_id
;




-- 2）、查询所有学生的学号、姓名、选课数、总成绩；

  With  tmp as  ( select  score_info.stu_id
         ,count(course_id) As count_course
    ,sum(score) As cnt_score

    from hive_sql_zg5.score_info
    GROUP BY  stu_id
    )
  select
      t1.stu_id,t2.stu_name,t1.cnt_score,t1.count_course
      from tmp t1
      left join hive_sql_zg5.student_info t2
  on t1.stu_id =t2.stu_id;




-- 3）、查询平均成绩大于85的所有学生的学号、姓名和平均成绩；

With  tmp1 as  ( select  stu_id
                     ,round(avg(score),2 )As avg_score

                from hive_sql_zg5.score_info
                GROUP BY  stu_id
                having  round(avg(score),2 )As avg_score>85
)
select
    t1.stu_id,t2.stu_name,t1.avg_score
from tmp1 t1
         left join hive_sql_zg5.student_info t2
                   on t1.stu_id =t2.stu_id;


-- todo：4.2、多表连接
-- 1）、课程编号为"01"且课程分数小于60，按分数降序排列的学生信息；

With  tmp1 as  (
            select  stu_id
                    ,course_id
                    ,score

                 from hive_sql_zg5.score_info
                where course_id ='01' AND score <60
        )

select
     t1.*,t2.stu_name,t2.birthday,t2.sex
    from tmp1 t1
left join  hive_sql_zg5.student_info  t2 on t1.stu_id =t2.stu_id
order by  t1.score DESC


-- 2）、查询所有课程成绩在70分以上的学生的姓名、课程名称和分数，按分数升序排列；
WITH
    tmp1 AS (
        SELECT
            stu_id, course_id, score
        FROM hive_sql_zg5.score_info
        WHERE stu_id IN (
            SELECT
                stu_id
            FROM hive_sql_zg5.score_info
            GROUP BY stu_id
            HAVING min(score) > 70
        )
    )
SELECT
    t1.stu_id
     , t2.stu_name
     , t1.course_id
     , t3.course_name
     , t1.score
FROM tmp1 t1
         LEFT JOIN hive_sql_zg5.student_info t2 ON t1.stu_id = t2.stu_id
         LEFT JOIN hive_sql_zg5.course_info t3 ON t1.course_id = t3.course_id
;







-- 3）、查询该学生不同课程的成绩相同的学生编号、课程编号、学生成绩；
select  t1.stu_id
     ,t1.course_id
     ,t1,score
from hive_sql_zg5.score_info  t1
join  hive_sql_zg5.score_info t2
on t1.stu_id =t2.stu_id AND  t1.course_id!= t2.course_id
AND  t1.score =t2.score;





-- 4）、查询课程编号为“01”的课程比“02”的课程成绩高的所有学生的学号；
With  tmp1 as  (
         select  stu_id
         ,sum(if(course_id ='01',score,0))  As score_01
         ,sum(if(course_id ='02',score,0))  As score_02

         from hive_sql_zg5.score_info
    group by  stu_id
)
select
   stu_id
,score_01
,score_02
from tmp1
where  score_01>score_02;

-- 5）、查询学过编号为“01”的课程并且也学过编号为“02”的课程的学生的学号、姓名；
With  tmp1 as  (
    select  stu_id
        ,collect_set(course_id) As c_course_id

    from hive_sql_zg5.score_info
    where  course_id in ('01','02')
    group by  stu_id
)
select
     stu_id
    from tmp1
where  array_contains(c_course_id,'01') AND array_contains(c_course_id,'02');











