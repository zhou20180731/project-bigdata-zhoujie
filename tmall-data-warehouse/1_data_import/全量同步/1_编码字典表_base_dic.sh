  # ======================================================================
  #                   todo：1. 编码字典表：base_dic（每日，全量）
  # ======================================================================
  /opt/module/sqoop/bin/sqoop import \
  --connect jdbc:mysql://node101:3306/gmall \
  --username root \
  --password 123456 \
  --target-dir /origin_data/gmall/base_dic_full/2024-06-18 \
  --delete-target-dir \
  --query "SELECT
    dic_code,
    dic_name,
    parent_code,
    create_time,
    operate_time
  FROM base_dic
  WHERE 1 = 1 AND \$CONDITIONS" \
  --num-mappers 1 \
  --split-by 'dic_code' \
  --fields-terminated-by '\t' \
  --compress \
  --compression-codec gzip \
  --null-string '\\N' \
  --null-non-string '\\N'



#CREATE TABLE `base_dic` (
#  `dic_code` varchar(10) NOT NULL COMMENT '编号',
#  `dic_name` varchar(100) DEFAULT NULL COMMENT '编码名称',
#  `parent_code` varchar(10) DEFAULT NULL COMMENT '父编号',
#  `create_time` datetime DEFAULT NULL COMMENT '创建日期',
#  `operate_time` datetime DEFAULT NULL COMMENT '修改日期'
#) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;