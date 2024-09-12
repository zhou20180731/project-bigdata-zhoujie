
-- 创建数据库
CREATE DATABASE IF NOT EXISTS hive_sql_zg5;

-- 使用数据库
USE hive_sql_zg5 ;


select hive_sql_zg5;

-- 创建学生表
DROP TABLE IF EXISTS student_info;
CREATE TABLE IF NOT EXISTS student_info(
    stu_id STRING COMMENT '学生id',
    stu_name STRING COMMENT '学生姓名',
    birthday STRING COMMENT '出生日期',
    sex STRING COMMENT '性别'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 创建课程表
DROP TABLE IF EXISTS course_info;
CREATE TABLE IF NOT EXISTS course_info(
    course_id STRING COMMENT '课程id',
    course_name STRING COMMENT '课程名',
    tea_id STRING COMMENT '任课老师id'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 创建老师表
DROP TABLE IF EXISTS teacher_info;
CREATE TABLE IF NOT EXISTS teacher_info(
    tea_id STRING COMMENT '老师id',
    tea_name STRING COMMENT '老师姓名'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 创建分数表
DROP TABLE IF EXISTS score_info;
CREATE TABLE IF NOT EXISTS score_info(
    stu_id STRING COMMENT '学生id',
    course_id STRING COMMENT '课程id',
    score DECIMAL(16, 2) COMMENT '成绩'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- 查看表
SHOW TABLES IN hive_sql_zg5 ;


-- 插入数据，提前将数据文件放到hdfs根目录/
LOAD DATA INPATH '/student_info.txt' INTO TABLE student_info;
LOAD DATA INPATH '/course_info.txt' INTO TABLE course_info;
LOAD DATA INPATH '/teacher_info.txt' INTO TABLE teacher_info;
LOAD DATA INPATH '/score_info.txt' INTO TABLE score_info;

--

-- 查询数据
SELECT * FROM student_info LIMIT 5;
SELECT * FROM course_info LIMIT 5;
SELECT * FROM teacher_info LIMIT 5;
SELECT * FROM score_info LIMIT 5;

