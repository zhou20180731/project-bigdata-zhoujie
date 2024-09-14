 # ======================================================================
  #                   todo：4品牌表base_trademark（每日，全量）
  # ======================================================================
  /opt/module/sqoop/bin/sqoop import \
  --connect jdbc:mysql://node101:3306/gmall \
  --username root \
  --password 123456 \
  --target-dir /origin_data/gmall/base_trademark_full/2024-06-18 \
  --delete-target-dir \
  --query "SELECT
    id,
    tm_name,
    logo_url
  FROM base_trademark
  WHERE 1 = 1 AND \$CONDITIONS" \
  --num-mappers 1 \
  --split-by 'id' \
  --fields-terminated-by '\t' \
  --compress \
  --compression-codec gzip \
  --null-string '\\N' \
  --null-non-string '\\N'

CREATE TABLE `base_trademark` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
  `tm_name` varchar(100) NOT NULL COMMENT '属性值',
  `logo_url` varchar(200) DEFAULT NULL COMMENT '品牌logo的图片路径',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='品牌表';