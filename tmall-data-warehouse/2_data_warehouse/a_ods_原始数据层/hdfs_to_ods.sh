#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=`date -d "-1 day" +%F`
fi

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


case $1 in
    "ods_base_trademark"){
        hive -e "${ods_base_trademark_sql}"
    };;
    "ods_base_category3"){
        hive -e "${ods_base_category3_sql}"
    };;
    "ods_base_category2"){
        hive -e "${ods_base_category2_sql}"
    };;
    "all"){
        hive -e "${ods_base_trademark_sql}${ods_base_category3_sql}${ods_base_category2_sql}"
    };;
esac


#    todo 全量：
#           todo：2. 品牌表：base_trademark（每日，全量）
#            todo：3. 三级分类表：base_category3（每日，全量）
#                todo： 4. 二级分类表：base_category2（每日，全量）

#step1. 执行权限
# chmod u+x hdfs_to_ods.sh
#step2. 前一天数据，加载到某张表
# hdfs_to_ods.sh ods_order_detail
#step3. 某天数据，加载到某张表
# hdfs_to_ods.sh ods_order_detail 2024-01-06
 # sh hdfs_to_ods.sh ods_base_trademark  2024-06-19
#step4. 某天数据，加载所有表
# sh hdfs_to_ods.sh all 2024-06-19
#
