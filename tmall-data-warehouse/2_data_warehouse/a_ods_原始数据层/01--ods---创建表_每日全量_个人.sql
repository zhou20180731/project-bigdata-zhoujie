
/*
	开发规范：
	1）、外部表
	2）、分区表（除了ADS层）
	3）、数据文件压缩（除了ADS层）
	4）、文件存储格式（除了ODS和ADS层）
	5）、字段命名：单词全部小写，使用下划线分割
	6）、字段类型：STRING、BIGINT、DECIMAL(16, 2)
		CAST(column_name AS DataType)
*/
-- 创建数据库
CREATE DATABASE IF NOT EXISTS gmall ;
-- 使用数据库
USE gmall;
-- 不开启自动收集表统计信息
SET hive.stats.autogather=false;

/*
	todo ODS层开发规范：
        （1）ODS层的表结构设计依托于从业务系统同步过来的数据结构；
        （2）ODS层要保存全部历史数据，故其压缩格式应选择压缩比较高的，此处选择gzip；
        （3）ODS层表名的命名规范为：ods_表名；
        （4）ODS层表：外部表、分区表（日期）；
*/



-- ======================================================================
--               todo：2. 品牌表：base_trademark（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_base_trademark_full;
CREATE EXTERNAL TABLE gmall.ods_base_trademark_full (
   `id` STRING COMMENT '编号',
   `tm_name` STRING COMMENT '属性值',
   `logo_url` STRING COMMENT '品牌logo的图片路径'
)  COMMENT '品牌表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/';


-- ======================================================================
--                todo：3. 三级分类表：base_category3（每日，全量）
-- ======================================================================

DROP TABLE IF EXISTS gmall.ods_base_category3_full;
CREATE EXTERNAL TABLE gmall.ods_base_category3_full (
  `id` STRING COMMENT '编号',
  `name` STRING COMMENT '三级分类名称',
  `category2_id` STRING COMMENT '二级分类编号'
)  COMMENT '三级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_category3_full/';



-- ======================================================================
--             todo： 4. 二级分类表：base_category2（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_base_category2_full;
CREATE EXTERNAL TABLE gmall.ods_base_category2_full(
 `id` STRING COMMENT '编号',
 `name` STRING COMMENT '级分类名称',
 `category1_id` STRING COMMENT '一级分类编号'
) COMMENT '商品二级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_category2_full/';

