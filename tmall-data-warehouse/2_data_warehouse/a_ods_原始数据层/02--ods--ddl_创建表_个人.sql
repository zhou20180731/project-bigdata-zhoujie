
-- 创建数据库
CREATE DATABASE IF NOT EXISTS gmall ;
-- 使用数据库
USE gmall;
---导入数据进日志表中
LOAD DATA INPATH '/origin_data/gmall/log/2024-09-11'
    OVERWRITE INTO TABLE gmall.ods_log_inc  PARTITION(dt='2024-09-11');



-- todo 1. 编码字典表：base_dic（每日，全量）
LOAD DATA INPATH '/origin_data/gmall/base_dic_full/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_base_dic_full PARTITION (dt = '2024-06-18');

-- 显示分区数目
SHOW PARTITIONS gmall.ods_base_dic_full ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_base_dic_full WHERE dt = '2024-06-18' LIMIT 10 ;



-- todo 2. 品牌表：base_trademark（每日，全量） gmall.ods_base_trademark_full
LOAD DATA INPATH '/origin_data/gmall/base_trademark_full/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_base_trademark_full PARTITION (dt = '2024-06-18');

-- 显示分区数目
SHOW PARTITIONS gmall.ods_base_trademark_full ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_base_trademark_full WHERE dt = '2024-06-18' LIMIT 10 ;




-- todo 3. 三级分类表：base_category3（每日，全量） gmall.ods_base_category3_full

LOAD DATA INPATH '/origin_data/gmall/base_category3_full/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_base_category3_full PARTITION (dt = '2024-06-18');

-- 显示分区数目
SHOW PARTITIONS gmall.ods_base_category3_full ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_base_category3_full WHERE dt = '2024-06-18' LIMIT 10 ;



-- todo 4. 二级分类表：base_category2（每日，全量） gmall.ods_base_category2_full

LOAD DATA INPATH '/origin_data/gmall/base_trademark_full/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_base_category2_full PARTITION (dt = '2024-06-18');

-- 显示分区数目
SHOW PARTITIONS gmall.ods_base_category2_full ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_base_category2_full WHERE dt = '2024-06-18' LIMIT 10 ;




-- ==========================================================================
-- ==========================================================================

-- todo: 1.订单详情表：order_detail（每日，增量）
LOAD DATA INPATH '/origin_data/gmall/order_detail_inc/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_order_detail_inc PARTITION (dt = '2024-06-18');
-- 显示分区数目
SHOW PARTITIONS gmall.ods_order_detail_inc ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_order_detail_inc WHERE dt = '2024-06-18' LIMIT 10 ;


-- todo 5.订单明细购物券表：order_detail_coupon（每日，增量）周洁
LOAD DATA INPATH '/origin_data/gmall/order_detail_coupon_inc/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_order_detail_inc PARTITION (dt = '2024-06-18');
-- 显示分区数目
SHOW PARTITIONS gmall.ods_order_detail_inc ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_order_detail_inc WHERE dt = '2024-06-18' LIMIT 10 ;




-- todo 6.商品评论表：comment_info（每日，增量）周洁
LOAD DATA INPATH '/origin_data/gmall/comment_info_inc/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_comment_info_inc PARTITION (dt = '2024-06-18');
-- 显示分区数目
SHOW PARTITIONS gmall.ods_comment_info_inc ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_comment_info_inc WHERE dt = '2024-06-18' LIMIT 10 ;




-- todo 7. 商品收藏表：favor_info（每日，增量）周洁
LOAD DATA INPATH '/origin_data/gmall/favor_info_inc/2024-06-18'
    OVERWRITE INTO TABLE gmall.ods_favor_info_inc PARTITION (dt = '2024-06-18');
-- 显示分区数目
SHOW PARTITIONS gmall.ods_favor_info_inc ;
-- 查询分区表数据，where过滤日期，limit限制条目数
SELECT * FROM gmall.ods_favor_info_inc WHERE dt = '2024-06-18' LIMIT 10 ;


