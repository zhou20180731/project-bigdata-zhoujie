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
CREATE DATABASE IF NOT EXISTS gmall;
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
--             todo：1. 编码字典表：base_dic（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_base_dic_full;
CREATE EXTERNAL TABLE gmall.ods_base_dic_full
(
    `dic_code`     STRING COMMENT '编号',
    `dic_name`     STRING COMMENT '编码名称',
    `parent_code`  STRING COMMENT '父编码',
    `create_time`  STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '操作日期'
) COMMENT '编码字典表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_dic_full/';


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


- ======================================================================
--                todo：5. 一级分类表：base_category1（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_base_category1_full;
CREATE EXTERNAL TABLE gmall.ods_base_category1_full
(
    `id`   STRING COMMENT 'id',
    `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_category1_full/';


-- ======================================================================
--           todo：6. 库存单元表（商品信息表）：sku_info（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_sku_info_full;
CREATE EXTERNAL TABLE gmall.ods_sku_info_full
(
    `id`                STRING COMMENT '库存id(itemID)',
    `spu_id`            STRING COMMENT '商品id',
    `price`             STRING COMMENT '价格',
    `sku_name`          STRING COMMENT 'sku名称',
    `sku_desc`          STRING COMMENT '商品规格描述',
    `weight`            STRING COMMENT '重量',
    `tm_id`             STRING COMMENT '品牌(冗余)',
    `category3_id`      STRING COMMENT '三级分类id（冗余)',
    `sku_default_img`   STRING COMMENT '默认显示图片(冗余)',
    `is_sale`           STRING COMMENT '是否销售（1：是 0：否）',
    `create_time`       STRING COMMENT '创建时间'
) COMMENT '商品信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_sku_info_full/';

-- ======================================================================
--            todo：7. 商品表：spu_info（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_spu_info_full;
CREATE EXTERNAL TABLE gmall.ods_spu_info_full
(
    `id`           STRING COMMENT 'spuid',
    `spu_name`     STRING COMMENT 'spu名称',
    `description`  STRING COMMENT '商品描述（后台简述）',
    `category3_id` STRING COMMENT '品类id',
    `tm_id`        STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_spu_info_full/';

-- ======================================================================
--                    todo：8. 活动表：activity_info（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_activity_info_full;
CREATE EXTERNAL TABLE gmall.ods_activity_info_full
(
    `id`            STRING COMMENT '编号',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time`    STRING COMMENT '开始时间',
    `end_time`      STRING COMMENT '结束时间',
    `create_time`   STRING COMMENT '创建时间'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_activity_info_full/';


-- ======================================================================
--                    todo：9. 优惠规则：activity_rule（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_activity_rule_full;
CREATE EXTERNAL TABLE gmall.ods_activity_rule_full
(
    `id`               STRING COMMENT '编号',
    `activity_id`      STRING COMMENT '活动ID',
    `activity_type`    STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`    BIGINT COMMENT '满减件数',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_level`    STRING COMMENT '优惠级别'
) COMMENT '活动规则表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_activity_rule_full/';


-- ======================================================================
--                 todo：10. 购物车表：cart_info（每日，全量）
-- ======================================================================
-- DROP TABLE IF EXISTS gmall.ods_cart_info_full;
-- CREATE EXTERNAL TABLE gmall.ods_cart_info_full
-- (
--     `id`           STRING COMMENT '编号',
--     `user_id`      STRING COMMENT '用户id',
--     `sku_id`       STRING COMMENT 'skuid',
--     `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
--     `sku_num`      BIGINT COMMENT '数量',
--     `img_url`      STRING COMMENT '托文件',
--     `sku_name`     STRING COMMENT 'sku名称 (冗余)',
--     `is_checked`   STRING COMMENT '是否检查',
--     `create_time`  STRING COMMENT '创建时间',
--     `operate_time` STRING COMMENT '修改时间',
--     `is_ordered`   STRING COMMENT '是否已经下单',
--     `order_time`   STRING COMMENT '下单时间',
--     `source_type`  STRING COMMENT '来源类型',
--     `source_id`    STRING COMMENT '来源编号'
-- ) COMMENT '加购表'
--     PARTITIONED BY (`dt` STRING)
--     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
--     STORED AS TEXTFILE
--     LOCATION '/warehouse/gmall/ods/ods_cart_info_full/';


-- ======================================================================
--                    todo：11.  优惠券表：coupon_info（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_coupon_info_full;
CREATE EXTERNAL TABLE gmall.ods_coupon_info_full
(
    `id`               STRING COMMENT '购物券编号',
    `coupon_name`      STRING COMMENT '购物券名称',
    `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`    BIGINT COMMENT '满件数',
    `activity_id`      STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `create_time`      STRING COMMENT '创建时间',
    `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num`        BIGINT COMMENT '最多领用次数',
    `taken_count`      BIGINT COMMENT '已领用次数',
    `start_time`       STRING COMMENT '开始领取时间',
    `end_time`         STRING COMMENT '结束领取时间',
    `operate_time`     STRING COMMENT '修改时间',
    `expire_time`      STRING COMMENT '过期时间',
    `range_desc`       STRING COMMENT '范围描述'
) COMMENT '优惠券表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_coupon_info_full/';


-- ======================================================================
--                todo：12. sku平台属性值关联表：sku_attr_value（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_sku_attr_value_full;
CREATE EXTERNAL TABLE gmall.ods_sku_attr_value_full
(
    `id`         STRING COMMENT '编号',
    `attr_id`    STRING COMMENT '平台属性ID',
    `value_id`   STRING COMMENT '平台属性值ID',
    `sku_id`     STRING COMMENT '商品ID',
    `attr_name`  STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_sku_attr_value_full/';


-- ======================================================================
--          todo：13. sku销售属性值：sku_sale_attr_value（每日，全量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_sku_sale_attr_value_full;
CREATE EXTERNAL TABLE gmall.ods_sku_sale_attr_value_full
(
    `id`                   STRING COMMENT '编号',
    `sku_id`               STRING COMMENT 'sku_id',
    `spu_id`               STRING COMMENT 'spu_id',
    `sale_attr_value_id`   STRING COMMENT '销售属性值id',
    `sale_attr_id`         STRING COMMENT '销售属性id',
    `sale_attr_name`       STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/';


-- ======================================================================
--                todo：14. 省份表：base_province（一次全量加载）
-- ======================================================================


DROP TABLE IF EXISTS gmall.ods_base_province_full;
CREATE TABLE gmall.ods_base_province_full (
          `id`              STRING COMMENT 'id',
          `name`            STRING COMMENT '省名称',
          `region_id`       STRING COMMENT '大区id',
          `area_code`       STRING COMMENT '行政区位码',
          `iso_code`        STRING COMMENT '国际编码',
          `iso_3166_2`      STRING COMMENT 'ISO3166编码'
)  COMMENT '省份表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_province_full/';;

-- ======================================================================
--         todo：15. 区域表：base_region（一次全量加载）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_base_region_full;
CREATE EXTERNAL TABLE gmall.ods_base_region_full
(
    `id`          STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
) COMMENT '地区表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_base_region_full/';


-- 创建数据库
CREATE DATABASE IF NOT EXISTS gmall;
-- 使用数据库
USE gmall;


-- ======================================================================
--                   todo: 1.订单详情表：order_detail（每日，增量）
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
--                   todo: 2.退单表：order_refund_info（每日，增量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_order_refund_info_inc;
CREATE EXTERNAL TABLE gmall.ods_order_refund_info_inc
(
    `id`                 STRING COMMENT '编号',
    `user_id`            STRING COMMENT '用户ID',
    `order_id`           STRING COMMENT '订单ID',
    `sku_id`             STRING COMMENT '商品ID',
    `refund_type`        STRING COMMENT '退单类型',
    `refund_num`         BIGINT COMMENT '退单件数',
    `refund_amount`      DECIMAL(16, 2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `refund_reason_txt`  STRING COMMENT '原因内容',
    --退单状态应包含买家申请、卖家审核、卖家收货、退款完成等状态。此处未涉及到，故该表按增量处理
    `refund_status`      STRING COMMENT '退单状态',
    `create_time`        STRING COMMENT '退单时间'
) COMMENT '退单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_refund_info_inc/';


-- ======================================================================
--           todo: 3.订单状态日志表：order_status_log（每日，增量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_order_status_log_inc;
CREATE EXTERNAL TABLE gmall.ods_order_status_log_inc
(
    `id`           STRING COMMENT '编号',
    `order_id`     STRING COMMENT '订单ID',
    `order_status` STRING COMMENT '订单状态',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '订单状态表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_status_log_inc/';


-- ======================================================================
--          todo: 4.订单明细活动关联表：order_detail_activity（每日，增量）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_order_detail_activity_inc;
CREATE EXTERNAL TABLE gmall.ods_order_detail_activity_inc
(
    `id`               STRING COMMENT '编号',
    `order_id`         STRING COMMENT '订单号',
    `order_detail_id`  STRING COMMENT '订单明细id',
    `activity_id`      STRING COMMENT '活动id',
    `activity_rule_id` STRING COMMENT '活动规则id',
    `sku_id`           BIGINT COMMENT '商品id',
    `create_time`      STRING COMMENT '创建时间'
) COMMENT '订单详情活动关联表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_detail_activity_inc/';


-- ======================================================================
--           todo: 5.订单明细购物券表：order_detail_coupon（每日，增量）
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


-- ======================================================================
--               todo： 8.订单表：order_info（每日，新增变化）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_order_info_inc;
CREATE EXTERNAL TABLE gmall.ods_order_info_inc
(
    `id`                     STRING COMMENT '订单号',
    `consignee`              STRING COMMENT '收货人',
    `consignee_tel`          STRING COMMENT '收货人联系方式',
    `total_amount`           DECIMAL(16, 2) COMMENT '订单最终金额',
    `order_status`           STRING COMMENT '订单状态',
    `user_id`                STRING COMMENT '用户id',
    `payment_way`            STRING COMMENT '支付方式',
    `delivery_address`       STRING COMMENT '送货地址',
    `order_comment`          STRING COMMENT '订单备注',
    `out_trade_no`           STRING COMMENT '支付流水号',
    `trade_body`             STRING COMMENT '订单描述（第3方支付使用）',
    `create_time`            STRING COMMENT '创建时间',
    `operate_time`           STRING COMMENT '操作时间',
    `expire_time`            STRING COMMENT '过期时间',
    `process_status`         STRING COMMENT '进度状态',
    `tracking_no`            STRING COMMENT '物流单编号',
    `parent_order_id`        STRING COMMENT '父订单编号',
    `img_url`                STRING COMMENT '图片路径',
    `province_id`            STRING COMMENT '省份ID',
    `activity_reduce_amount` DECIMAL(16, 2) COMMENT '活动减免金额',
    `coupon_reduce_amount`   DECIMAL(16, 2) COMMENT '优惠券减免金额',
    `original_total_amount`  DECIMAL(16, 2) COMMENT '订单原价金额',
    `feight_fee`             DECIMAL(16, 2) COMMENT '运费',
    `feight_fee_reduce`      DECIMAL(16, 2) COMMENT '运费减免',
    `refundable_time`        STRING COMMENT '可退款日期（签收后30天）'
) COMMENT '订单表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_order_info_inc/';


-- ======================================================================
--               todo： 9.优惠券领用表：coupon_use（每日，新增变化）
-- ======================================================================
-- step1. 创建表
DROP TABLE IF EXISTS gmall.ods_coupon_use_inc;
CREATE EXTERNAL TABLE gmall.ods_coupon_use_inc
(
    `id`            STRING COMMENT '编号',
    `coupon_id`     STRING COMMENT '优惠券ID',
    `user_id`       STRING COMMENT 'skuid',
    `order_id`      STRING COMMENT 'spuid',
    `coupon_status` STRING COMMENT '优惠券状态',
    `create_time`   STRING COMMENT '创建时间',
    `get_time`      STRING COMMENT '领取时间',
    `using_time`    STRING COMMENT '使用时间(下单)',
    `used_time`     STRING COMMENT '使用时间(支付)',
    `expire_time`   STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/';

-- ======================================================================
--               todo： 10.用户表：user_info（每日，新增变化）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_user_info_inc;
CREATE EXTERNAL TABLE gmall.ods_user_info_inc
(
    `id`            STRING COMMENT '编号',
    `login_name`    STRING COMMENT '用户名称',
    `nick_name`     STRING COMMENT '用户昵称',
    `passwd`        STRING COMMENT '用户密码',
    `name`          STRING COMMENT '用户姓名',
    `phone_num`     STRING COMMENT '手机号',
    `email`         DECIMAL(16, 2) COMMENT '邮箱',
    `head_img`      STRING COMMENT '头像',
    `user_level`    STRING COMMENT '用户级别',
    `birthday`      STRING COMMENT '用户生日',
    `gender`        STRING COMMENT '性别 M男,F女',
    `create_time`   STRING COMMENT '创建时间',
    `operate_time`  STRING COMMENT '修改时间',
    `status`        STRING COMMENT '状态'
) COMMENT '用户表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_user_info_inc/';

-- ======================================================================
--               todo： 11.支付信息表：payment_info（每日，新增变化）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_payment_info_inc;
CREATE EXTERNAL TABLE gmall.ods_payment_info_inc
(
    `id`               STRING COMMENT '编号',
    `out_trade_no`     STRING COMMENT '对外业务编号',
    `order_id`         STRING COMMENT '订单编号',
    `user_id`          STRING COMMENT '用户编号',
    `payment_type`     STRING COMMENT '支付类型',
    `trade_no`         STRING COMMENT '交易编号',
    `total_amount`     DECIMAL(16, 2) COMMENT '支付金额',
    `subject`          STRING COMMENT '交易内容',
    `payment_status`   STRING COMMENT '支付状态',
    `create_time`      STRING COMMENT '创建时间',
    `callback_time`    STRING COMMENT '回调时间',
    `callback_content` STRING COMMENT '回调内容'
) COMMENT '支付流水表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_payment_info_inc/';

-

-- ======================================================================
--               todo： 12.退款信息表：refund_payment（每日，新增变化）
-- ======================================================================
-- step1. 创建表
DROP TABLE IF EXISTS gmall.ods_refund_payment_inc;
CREATE EXTERNAL TABLE gmall.ods_refund_payment_inc
(
    `id`               STRING COMMENT '编号',
    `out_trade_no`     STRING COMMENT '对外业务编号',
    `order_id`         STRING COMMENT '订单编号',
    `sku_id`           STRING COMMENT 'SKU编号',
    `payment_type`     STRING COMMENT '支付类型',
    `trade_no`         STRING COMMENT '交易编号',
    `total_amount`     DECIMAL(16, 2) COMMENT '支付金额',
    `subject`          STRING COMMENT '交易内容',
    `refund_status`    STRING COMMENT '支付状态',
    `create_time`      STRING COMMENT '创建时间',
    `callback_time`    STRING COMMENT '回调时间',
    `callback_content` STRING COMMENT '回调信息'
) COMMENT '支付流水表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_refund_payment_inc/';



-- ======================================================================
--               todo： 13.购物车表：cart_info（每日，新增变化）
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_cart_info_inc;
CREATE EXTERNAL TABLE gmall.ods_cart_info_inc
(
    `id`           STRING COMMENT '编号',
    `user_id`      STRING COMMENT '用户id',
    `sku_id`       STRING COMMENT 'skuid',
    `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
    `sku_num`      BIGINT COMMENT '数量',
    `img_url`      STRING COMMENT '托文件',
    `sku_name`     STRING COMMENT 'sku名称 (冗余)',
    `is_checked`   STRING COMMENT '是否检查',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered`   STRING COMMENT '是否已经下单',
    `order_time`   STRING COMMENT '下单时间',
    `source_type`  STRING COMMENT '来源类型',
    `source_id`    STRING COMMENT '来源编号'
) COMMENT '加购表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '/warehouse/gmall/ods/ods_cart_info_inc/';


-- ======================================================================
--        9.18  创建 todo： app日志表
-- ======================================================================
DROP TABLE IF EXISTS gmall.ods_log_inc;
CREATE EXTERNAL TABLE gmall.ods_log_inc
(
    `common`   STRUCT<ar :STRING,ba :STRING,ch :STRING,is_new :STRING,md :STRING,mid :STRING,os :STRING,uid :STRING,vc
                      :STRING> COMMENT '公共信息',
    `page`     STRUCT<during_time :STRING,item :STRING,item_type :STRING,last_page_id :STRING,page_id
                      :STRING,source_type :STRING> COMMENT '页面信息',
    `actions`  ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
    `displays` ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                            :STRING>> COMMENT '曝光信息',
    `start`    STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms
                      :BIGINT> COMMENT '启动信息',
    `err`      STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息',
    `ts`       BIGINT COMMENT '时间戳'
) COMMENT '活动信息表'
    PARTITIONED BY (`dt` STRING)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
    LOCATION '/warehouse/gmall/ods/ods_log_inc/';





