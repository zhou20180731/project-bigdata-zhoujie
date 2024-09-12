
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

-- 2）、查询数学成绩不及格的学生和其对应的成绩，按照学号升序排序；



-- 3）、查询姓名中带“冰”的学生名单；


-- 4）、查询姓“王”老师的个数；


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



-- 2）、查询参加考试的学生个数；

-- todo 当数据量很大时，上述SQL容易产生数据倾斜，因为使用count distinct只有1个Reduce任务，采用group by + count优化
/*
    案例剖析：https://blog.51cto.com/u_16099335/6687050
*/




/**
-- 查询日志
[bwie@node101 ~]$ hv.sh beeline
    执行SQL语句，查看运行日志

-- 查询计划
explain SELECT ....
*/














