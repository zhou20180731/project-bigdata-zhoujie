 # ======================================================================
  #                   todo：5.购物车表 cart_info（每日，全量）
  # ======================================================================
  /opt/module/sqoop/bin/sqoop import \
  --connect jdbc:mysql://node101:3306/gmall \
  --username root \
  --password 123456 \
  --target-dir /origin_data/gmall/cart_info_full/2024-06-18 \
  --delete-target-dir \
  --query "SELECT
    id,
    user_id,
    sku_id,
    cart_price,
    sku_num,
    img_url,
    sku_name,
    is_checked,
    create_time,
    operate_time,
    is_ordered,
    order_time,
    source_type,
    source_id
  FROM cart_info
  WHERE 1 = 1 AND \$CONDITIONS" \
  --num-mappers 1 \
  --split-by 'id' \
  --fields-terminated-by '\t' \
  --compress \
  --compression-codec gzip \
  --null-string '\\N' \
  --null-non-string '\\N'



CREATE TABLE `cart_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `user_id` varchar(200) DEFAULT NULL COMMENT '用户id',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'skuid',
  `cart_price` decimal(10,2) DEFAULT NULL COMMENT '放入购物车时价格',
  `sku_num` int(11) DEFAULT NULL COMMENT '数量',
  `img_url` varchar(200) DEFAULT NULL COMMENT '图片文件',
  `sku_name` varchar(200) DEFAULT NULL COMMENT 'sku名称 (冗余)',
  `is_checked` int(1) DEFAULT NULL,
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间',
  `is_ordered` bigint(20) DEFAULT NULL COMMENT '是否已经下单',
  `order_time` datetime DEFAULT NULL COMMENT '下单时间',
  `source_type` varchar(20) DEFAULT NULL COMMENT '来源类型',
  `source_id` bigint(20) DEFAULT NULL COMMENT '来源编号',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=6051 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='购物车表 用户登录系统时更新冗余';