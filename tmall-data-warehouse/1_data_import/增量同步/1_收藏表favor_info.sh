# ======================================================================
#                   todo：1. 收藏表 favor_info（增量，历史）
# ======================================================================
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/gmall \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/favor_info_inc/2024-06-18 \
--delete-target-dir \
--query "SELECT
        id,
        user_id,
        sku_id,
        spu_id,
        is_cancel,
        create_time,
        cancel_time
FROM favor_info
WHERE date_format(create_time, '%Y-%m-%d') <= '2024-06-18' AND \$CONDITIONS" \
--num-mappers 1 \
--split-by 'id' \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'



# ======================================================================
#                   todo：1.收藏表 favor_infol（增量，每日）
# ======================================================================
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/gmall \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/favor_info_inc/2024-06-19 \
--delete-target-dir \
--query "SELECT
      id,
     user_id,
     sku_id,
     spu_id,
     is_cancel,
     create_time,
     cancel_time
FROM favor_info
WHERE date_format(create_time, '%Y-%m-%d') = '2024-06-19' and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'

CREATE TABLE `favor_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `user_id` bigint(20) DEFAULT NULL COMMENT '用户名称',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'skuid',
  `spu_id` bigint(20) DEFAULT NULL COMMENT '商品id',
  `is_cancel` varchar(1) DEFAULT NULL COMMENT '是否已取消 0 正常 1 已取消',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `cancel_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1803633288395108363 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='商品收藏表';