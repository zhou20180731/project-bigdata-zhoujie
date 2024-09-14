#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi


ods_order_detail_coupon_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_detail_coupon_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_order_detail_coupon_inc PARTITION (dt = '${do_date}');
"

ods_comment_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/comment_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_comment_info_inc PARTITION (dt = '${do_date}');
"

ods_favor_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/favor_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_favor_info_inc PARTITION (dt = '${do_date}');
"


case $1 in
    "ods_order_detail_coupon"){
        hive -e "${ods_order_detail_coupon_sql}"
    };;
    "ods_comment_info"){
        hive -e "${ods_comment_info_sql}"
    };;
  "ods_favor_info"){
          hive -e "${ods_favor_info_sql}"
      };;

    "all"){
        hive -e "${ods_order_detail_coupon_sql}${ods_comment_info_sql}${ods_favor_info_sql}"
    };;
esac

#  todo: 1.订单详情表：order_detail（每日，增量） 周洁
#          todo: 5.订单明细购物券表：order_detail_coupon（每日，增量） 周洁
#            todo: 6.商品评论表：comment_info（每日，增量）
#              todo：7. 商品收藏表：favor_info（每日，增量）

#step1. 执行权限
# chmod u+x hdfs_to_ods_init.sh
#step2. 前一天数据，加载到某张表
# hdfs_to_ods_init.sh ods_order_detail
#step3. 某天数据，加载到某张表
# sh hdfs_to_ods_init.sh ods_order_detail_coupon 2024-06-18
#step4. 某天数据，加载所有表
# sh hdfs_to_ods_init.sh all 2024-06-18
#