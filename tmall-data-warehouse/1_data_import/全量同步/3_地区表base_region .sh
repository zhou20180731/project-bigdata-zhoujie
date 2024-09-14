
  # ======================================================================
    #                   todo：3地区表base_region  （每日，全量）
    # ======================================================================
    /opt/module/sqoop/bin/sqoop import \
    --connect jdbc:mysql://node101:3306/gmall \
    --username root \
    --password 123456 \
    --target-dir /origin_data/gmall/base_region_full/2024-06-18 \
    --delete-target-dir \
    --query "SELECT
      id,
     region_name
    FROM base_region
    WHERE 1 = 1 AND \$CONDITIONS" \
    --num-mappers 1 \
    --split-by 'id' \
    --fields-terminated-by '\t' \
    --compress \
    --compression-codec gzip \
    --null-string '\\N' \
    --null-non-string '\\N'


#CREATE TABLE `base_region` (
#  `id` varchar(20) DEFAULT NULL COMMENT '大区id',
#  `region_name` varchar(20) DEFAULT NULL COMMENT '大区名称'
#) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;