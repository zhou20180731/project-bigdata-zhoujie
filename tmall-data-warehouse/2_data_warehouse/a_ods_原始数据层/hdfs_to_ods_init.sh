#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi

# ===========================================================================================
#                                 定义变量：各个表数据加载语句
# ===========================================================================================

# 订单信息表
ods_order_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_info_inc/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_order_info_inc PARTITION(dt='${do_date}');"

# 订单明细表
ods_order_detail_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_detail_inc/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_order_detail_inc PARTITION(dt='${do_date}');"

# order_refund_info
ods_order_refund_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_refund_info_inc/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_order_refund_info_inc PARTITION(dt='${do_date}'); "

# refund_payment
ods_refund_payment_sql="
LOAD DATA INPATH '/origin_data/${APP}/refund_payment_inc/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_refund_payment_inc PARTITION(dt='${do_date}'); "

ods_order_detail_coupon_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_detail_coupon_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_order_detail_coupon_inc PARTITION (dt='${do_date}');
"

ods_comment_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/comment_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_comment_info_inc PARTITION (dt='${do_date}');
"

ods_favor_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/favor_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_favor_info_inc PARTITION (dt='${do_date}');
"

# 加购表：
ods_cart_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/cart_info_inc/${do_date}'
  OVERWRITE INTO TABLE ${APP}.ods_cart_info_inc PARTITION(dt='${do_date}');"


#订单状态日志表
ods_order_status_log_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_status_log_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_order_status_log_inc PARTITION (dt='${do_date}');"

#订单明细获得关联表
ods_order_detail_activity_sql="
LOAD DATA INPATH '/origin_data/${APP}/order_detail_activity_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_order_detail_activity_inc PARTITION (dt='${do_date}');"

ods_coupon_use_sql="
LOAD DATA INPATH '/origin_data/${APP}/coupon_use_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_coupon_use_inc PARTITION (dt='${do_date}');
"

ods_user_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/user_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_user_info_inc PARTITION (dt='${do_date}');
"

ods_payment_info_sql="
LOAD DATA INPATH '/origin_data/${APP}/payment_info_inc/${do_date}'
    OVERWRITE INTO TABLE ${APP}.ods_payment_info_inc PARTITION (dt='${do_date}');
"








case $1 in
    "ods_order_info"){
        hive -e "${ods_order_info_sql}"
    };;
    "ods_order_detail"){
        hive -e "${ods_order_detail_sql}"
    };;
        "ods_order_refund_info"){
            hive -e "${ods_order_refund_info_sql}"
    };;
        "ods_refund_payment"){
            hive -e "${ods_refund_payment_sql}"
    };;
        "ods_cart_info"){
            hive -e "${ods_cart_info_sql}"
    };;
        "ods_order_status_log"){
            hive -e "${ods_order_status_log_sql}"
        };;
        "ods_order_detail_activity"){
            hive -e "${ods_order_detail_activity_sql}"
        };;
       "ods_coupon_use"){
              hive -e "${ods_coupon_use_sql}"
          };;
          "ods_user_info"){
              hive -e "${ods_user_info_sql}"
          };;
          "ods_payment_info"){
              hive -e "${ods_payment_info_sql}"
          };;
    "all"){
                  hive -e "
                  ${ods_user_info_sql};
                  ${ods_payment_info_sql};
                  ${ods_order_info_sql};
                  ${ods_order_detail_sql};
                  ${ods_order_refund_info_sql};
                  ${ods_refund_payment_sql};
                  ${ods_cart_info_sql};
                  ${ods_order_detail_coupon_sql};
                  ${ods_comment_info_sql};
                  ${ods_favor_info_sql};
                  ${ods_order_status_log_sql};
                  ${ods_order_detail_activity_sql};
                  ${ods_coupon_use_sql};"
              };;
esac

# ${ods_activity_rule_sql}${ods_base_province_sql}${ods_base_region_sql}
#step1. 执行权限
# chmod +x hdfs_to_ods_init.sh

#step2. 前一天数据，加载到某张表
# hdfs_to_ods_init.sh ods_order_detail
#step3. 某天数据，加载到某张表
# hdfs_to_ods_init.sh ods_order_detail 2024-04-18

#step4. 某天数据，加载所有表（全部）
# sh hdfs_to_ods_init.sh all 2024-06-18
#
