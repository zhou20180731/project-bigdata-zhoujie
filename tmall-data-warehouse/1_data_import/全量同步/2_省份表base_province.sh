# ======================================================================
  #                   todo：2. 地区表:base_region（每日，全量）
  # ======================================================================
  /opt/module/sqoop/bin/sqoop import \
  --connect jdbc:mysql://node101:3306/gmall \
  --username root \
  --password 123456 \
  --target-dir /origin_data/gmall/base_province_full/2024-06-18 \
  --delete-target-dir \
  --query "SELECT
    id,
    name,
    region_id,
    area_code,
    iso_code,
    iso_3166_2
  FROM base_province
  WHERE 1 = 1 AND \$CONDITIONS" \
  --num-mappers 1 \
  --split-by 'id' \
  --fields-terminated-by '\t' \
  --compress \
  --compression-codec gzip \
  --null-string '\\N' \
  --null-non-string '\\N'

#CREATE TABLE `base_province` (
#  `id` bigint(20) DEFAULT NULL COMMENT 'id',
#  `name` varchar(20) DEFAULT NULL COMMENT '省名称',
#  `region_id` varchar(20) DEFAULT NULL COMMENT '大区id',
#  `area_code` varchar(20) DEFAULT NULL COMMENT '行政区位码',
#  `iso_code` varchar(20) DEFAULT NULL COMMENT '国际编码',
#  `iso_3166_2` varchar(20) DEFAULT NULL COMMENT 'ISO3166编码'
#) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;