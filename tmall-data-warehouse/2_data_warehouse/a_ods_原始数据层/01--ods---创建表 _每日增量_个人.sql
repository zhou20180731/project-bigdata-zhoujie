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
--                   todo: 1.订单详情表：order_detail（每日，增量） 周洁
-- ======================================================================

DROP TABLE IF EXISTS gmall.ods_order_detail_inc;
CREATE EXTERNAL TABLE gmall.ods_order_detail_inc(
       `id` STRING COMMENT '编号',
       `order_id` STRING COMMENT '订单编号',
       `sku_id` STRING COMMENT 'sku_id',
       `sku_name` STRING COMMENT 'sku名称（冗余)',
       `img_url` STRING COMMENT '图片名称（冗余)',
       `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
       `sku_num` STRING  COMMENT '购买个数',
       `create_time` STRING COMMENT '创建时间',
       `source_type` STRING COMMENT '来源类型',
       `source_id` STRING  COMMENT '来源编号',
       `split_total_amount` decimal(16,2) ,
       `split_activity_amount` decimal(16,2) ,
       `split_coupon_amount` decimal(16,2)

) COMMENT '订单明细表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_detail_inc/';




-- ======================================================================
--           todo: 5.订单明细购物券表：order_detail_coupon（每日，增量） 周洁
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_order_detail_coupon_inc;
CREATE EXTERNAL TABLE gmall.ods_order_detail_coupon_inc(
            `id` STRING COMMENT '编号',
            `order_id` STRING  COMMENT '订单号',
            `order_detail_id` STRING COMMENT '订单明细id',
            `coupon_id` STRING COMMENT '优惠券id',
            `coupon_use_id` STRING COMMENT '优惠券领用记录id',
            `sku_id` STRING COMMENT '商品id',
            `create_time` STRING COMMENT '创建时间'
) COMMENT '订单详情活动关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/';




-- ======================================================================
--                   todo: 6.商品评论表：comment_info（每日，增量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_comment_info_inc;
CREATE EXTERNAL TABLE gmall.ods_comment_info_inc(
   `id` STRING COMMENT '编号',
   `user_id` STRING  COMMENT '订单号',
   `nick_name` STRING COMMENT '订单明细id',
   `head_img` STRING COMMENT '优惠券id',
   `sku_id` STRING COMMENT '优惠券领用记录id',
   `spu_id` STRING COMMENT '商品id',
    `order_id` STRING COMMENT '创建时间',
    `appraise` STRING COMMENT '评价 1 好评 2 中评 3 差评',
   `comment_txt` STRING COMMENT '评价内容',
   `create_time` STRING COMMENT '创建时间',
   `operate_time` STRING COMMENT '修改时间'

) COMMENT '商品评论表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_comment_info_inc/';


-- ======================================================================
--                   todo：7. 商品收藏表：favor_info（每日，增量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_favor_info_inc;
CREATE EXTERNAL TABLE gmall.ods_favor_info_inc(
   `id` STRING COMMENT '编号',
   `user_id` STRING COMMENT '用户id',
   `sku_id` STRING COMMENT 'skuid',
   `spu_id` STRING COMMENT 'spuid',
   `is_cancel` STRING COMMENT '是否取消',
   `create_time` STRING COMMENT '收藏时间',
      `cancel_time` STRING COMMENT '取消时间'
) COMMENT '商品收藏表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_favor_info_inc/';

