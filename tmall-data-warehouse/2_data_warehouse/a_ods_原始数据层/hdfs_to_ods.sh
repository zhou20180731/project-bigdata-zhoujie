#!/bin/bash

APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi

# ===========================================================================================
#                                 定义变量：各个表数据加载语句
# ===========================================================================================

# 字典表  全量
ods_base_dic_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_dic_full/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_base_dic_full PARTITION(dt='${do_date}'); "

# activity_rule
ods_activity_rule_sql="
LOAD DATA INPATH '/origin_data/${APP}/activity_rule_full/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_activity_rule_full PARTITION(dt='${do_date}');"

ods_base_province_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_province_full/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_base_province_full PARTITION(dt='${do_date}');"

ods_base_region_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_region_full/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_base_region_full PARTITION(dt='${do_date}');"


ods_base_trademark_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_trademark_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_base_trademark_full PARTITION (dt = '${do_date}');
"

ods_base_category3_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_category3_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_base_category3_full PARTITION (dt = '${do_date}');
"

ods_base_category2_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_category2_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_base_category2_full PARTITION (dt = '${do_date}');
"

#优惠券表
ods_coupon_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/coupon_info_full/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_coupon_info_full PARTITION(dt='${do_date}');"

#sku平台属性值关联表
ods_sku_attr_value_sql="
LOAD DATA INPATH '/origin_data/${APP}/sku_attr_value_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_sku_attr_value_full PARTITION (dt='${do_date}');"

#sku销售属性值
ods_sku_sale_attr_value_sql="
LOAD DATA INPATH '/origin_data/${APP}/sku_sale_attr_value_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_sku_sale_attr_value_full PARTITION (dt='${do_date}');"


ods_base_category1_sql="
LOAD DATA INPATH '/origin_data/${APP}/base_category1_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_base_category1_full PARTITION (dt = '${do_date}');
"

ods_sku_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/sku_info_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_sku_info_full PARTITION (dt = '${do_date}');
"

ods_spu_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/spu_info_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_spu_info_full PARTITION (dt = '${do_date}');
"

ods_activity_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/activity_info_full/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_activity_info_full PARTITION (dt = '${do_date}');
"

case $1 in
    "ods_base_dic"){
        hive -e "${ods_base_dic_sql}"
    };;
        "ods_activity_rule"){
            hive -e "${ods_activity_rule_sql}"
    };;
        "ods_base_province"){
            hive -e "${ods_base_province_sql}"
    };;
        "ods_base_region"){
            hive -e "${ods_base_region_sql}"
    };;
        "ods_base_trademark"){
          hive -e "${ods_base_trademark_sql}"
    };;
        "ods_base_category3"){
          hive -e "${ods_base_category3_sql}"
    };;
        "ods_base_category2"){
          hive -e "${ods_base_category2_sql}"
    };;
        "ods_coupon_info"){
          hive -e "${ods_coupon_info_sql}"
    };;
        "ods_sku_attr_value"){
          hive -e "${ods_sku_attr_value_sql}"
    };;
       "ods_sku_sale_attr_value"){
        hive -e "${ods_sku_sale_attr_value_sql}"
    };;
        "ods_base_category1"){
        hive -e "${ods_base_category1_sql}"
    };;
        "ods_sku_info"){
        hive -e "${ods_sku_info_sql}"
    };;
        "ods_spu_info"){
        hive -e "${ods_spu_info_sql}"
    };;
        "ods_activity_info"){
          hive -e "${ods_activity_info_sql}"
      };;
      "all"){
        hive -e "
        ${ods_base_dic_sql};
        ${ods_activity_rule_sql};
        ${ods_base_province_sql};
        ${ods_base_region_sql};
        ${ods_base_trademark_sql};
        ${ods_base_category3_sql};
        ${ods_base_category2_sql};
        ${ods_coupon_info_sql};
        ${ods_sku_attr_value_sql};
        ${ods_sku_sale_attr_value_sql};
        ${ods_base_category1_sql};
        ${ods_sku_info_sql};
        ${ods_spu_info_sql};
        ${ods_activity_info_sql};"
    };;
esac

#step1. 执行权限
# chmod +x hdfs_to_ods.sh

#step2. 前一天数据，加载到某张表
# hdfs_to_ods.sh ods_order_detail
#step3. 某天数据，加载到某张表
# hdfs_to_ods.sh ods_order_detail 2024-06-19

#step4. 某天数据，加载所有表
# sh hdfs_to_ods.sh all 2024-06-19
#
