#!/bin/bash

APP=gmall

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi

dwd_trade_cart_add_full="WITH cart_info AS
                         	     (
                         	     SELECT data.id,
                         	            data.user_id,
                         	            data.sku_id,
                         	            data.create_time,
                         	            data.source_id,
                         	            data.source_type,
                         	            data.sku_num
                         	     FROM ${APP}.ods_cart_info_full  data
                         	     WHERE dt = '${do_date}'
                         	     ),
                              bash_dic AS
                         	     (
                         	     SELECT dic_code,
                         	            dic_name
                         	     FROM ${APP}.ods_bash_dic_full
                         	     WHERE dt = '${do_date}'
                         		   AND parent_code = '24'
                         	     )
                         INSERT OVERWRITE TABLE ${APP}.dwd_trade_cart_add_inc PARTITION (dt='${do_date}')
                         SELECT id,
                                user_id,
                                sku_id,
                                date_format(create_time,'yyyy-MM-dd') date_id,
                                create_time,
                                source_id,
                                source_type,
                                dic.dic_name,
                                sku_num
                         FROM cart_info ci
                              LEFT JOIN bash_dic dic ON ci.source_type = dic.dic_code;"


dwd_trade_order_detail_full= "WITH
        -- a. 订单明细数据
        detail AS (
                  SELECT id,
                        order_id,
                        sku_id,
                        create_time,
                        source_id,
                        source_type,
                        sku_num,
                        sku_num * order_price AS split_original_amount,
                        split_total_amount,
                        split_activity_amount,
                        split_coupon_amount
                  FROM ${APP}.ods_order_detail_inc
                  WHERE dt <= '${do_date}'
                  ),
        -- b. 订单数据
        info AS (
                  SELECT id,
                        user_id,
                        province_id
                  FROM ${APP}.ods_order_info_inc
                  WHERE dt <= '${do_date}'
                  ),
        -- c. 订单明细活动关联表
        activity AS (
                  SELECT order_detail_id,
                        activity_id,
                        activity_rule_id
                  FROM ${APP}.ods_order_detail_activity_inc
                  WHERE dt <= '${do_date}'
                  ),
        -- d. 订单明细优惠卷关联表
        coupon AS (
                  SELECT order_detail_id,
                        coupon_id
                  FROM ${APP}.ods_order_detail_coupon_inc
                  WHERE dt <= '${do_date}'
                  ),
        -- e. 字典数据
        dic AS (
                  SELECT dic_code,
                        dic_name
                  FROM ${APP}.ods_bash_dic_full
                  WHERE dt <= '${do_date}'
                  )
        INSERT
        OVERWRITE
        TABLE
        ${APP}.dwd_trade_order_detail_inc
        PARTITION
        (
        dt = '${do_date}'
        )
        -- f. 订单明细表关联订单info、活动activity、优惠卷coupon、字段dic
        SELECT info.id,
              order_id,
              user_id,
              sku_id,
              province_id,
              activity_id,
              activity_rule_id,
              coupon_id,
              date_format(create_time, 'yyyy-MM-dd') AS date_id,
              create_time,
              source_id,
              source_type,
              dic_name,
              sku_num,
              split_original_amount,
              split_activity_amount,
              split_coupon_amount,
              split_total_amount
        FROM detail
            LEFT JOIN info ON detail.order_id = info.id
            LEFT JOIN activity ON detail.id = activity.order_detail_id
            LEFT JOIN coupon ON detail.id = coupon.order_detail_id
            LEFT JOIN dic ON detail.source_type = dic.dic_code;"


dwd_trade_pay_detail_suc_full="
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.stats.autogather=false;
WITH od as (SELECT id,
                   order_id,
                   sku_id,
                   source_id,
                   source_type,
                   sku_num,
                   sku_num * order_price split_original_amount,
                   split_total_amount,
                   split_activity_amount,
                   split_coupon_amount
            FROM ${APP}.ods_order_detail_inc
            WHERE dt = '${do_date}')
        ,
     pi as (SELECT user_id,
                   order_id,
                   payment_type,
                   callback_time
            FROM ${APP}.ods_payment_info_inc
            WHERE dt = '${do_date}'
              and payment_status = '1602')
        ,
     oi as (SELECT id
                 , consignee
                 , consignee_tel
                 , total_amount
                 , order_status
                 , user_id
                 , payment_way
                 , delivery_address
                 , order_comment
                 , out_trade_no
                 , trade_body
                 , create_time
                 , operate_time
                 , expire_time
                 , process_status
                 , tracking_no
                 , parent_order_id
                 , img_url
                 , province_id
                 , activity_reduce_amount
                 , coupon_reduce_amount
                 , original_total_amount
                 , feight_fee
                 , feight_fee_reduce
                 , dt
            FROM ${APP}.ods_order_info_inc
            WHERE dt = '${do_date}')
        ,
     act as (SELECT order_detail_id,
                    activity_id,
                    activity_rule_id
             FROM ${APP}.ods_order_detail_activity_inc
             WHERE dt = '${do_date}'),
     cou as (SELECT order_detail_id,
                    coupon_id
             FROM ${APP}.ods_order_detail_coupon_inc
             WHERE dt = '${do_date}')
        ,
     pay_dic as (SELECT dic_code,
                        dic_name
                 FROM ${APP}.ods_base_dic_full
                 WHERE dt = '${do_date}'
                   and parent_code = '11')
        ,
     src_dic as (SELECT dic_code,
                        dic_name
                 FROM ${APP}.ods_base_dic_full
                 WHERE dt = '${do_date}'
                   and parent_code = '24')
INSERT
OVERWRITE
TABLE
${APP}.dwd_trade_pay_detail_suc_inc
PARTITION
(
dt
)
SELECT od.id,
       od.order_id,
       oi.user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       pi.payment_type,
       pay_dic.dic_name,
       date_format(pi.callback_time, 'yyyy-MM-dd') date_id,
       pi.callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       split_activity_amount,
       split_coupon_amount,
       split_total_amount,
       date_format(callback_time,'yyyy-MM-dd') as dt
FROM od
         LEFT JOIN pi ON od.order_id = pi.order_id
         LEFT JOIN oi ON od.order_id = oi.id
         LEFT JOIN act ON od.id = act.order_detail_id
         LEFT JOIN cou ON od.id = cou.order_detail_id
         LEFT JOIN pay_dic ON pi.payment_type = pay_dic.dic_code
         LEFT JOIN src_dic ON od.source_type = src_dic.dic_code;"



dwd_trade_cart_full="INSERT OVERWRITE TABLE ${APP}.dwd_trade_cart_full PARTITION (dt = '${do_date}')
                      SELECT id,
                            user_id,
                            sku_id,
                            sku_name,
                            sku_num
                      FROM ${APP}.ods_cart_info_full
                      WHERE dt <= '${do_date}'
                        and is_ordered = '0';"


dwd_tool_coupon_get_full="INSERT OVERWRITE TABLE ${APP}.dwd_tool_coupon_get_inc partition (dt = '${do_date}')
                          SELECT id
                                  , coupon_id
                                  , user_id
                                  , date_format(get_time, 'yyyy-MM-dd') date_id
                                  , get_time
                          FROM ${APP}.ods_coupon_use_inc
                          WHERE dt <= '${do_date}';"


dwd_tool_coupon_order_full="INSERT OVERWRITE TABLE ${APP}.dwd_tool_coupon_order_inc PARTITION (dt = '${do_date}')
                            SELECT id,
                                    coupon_id,
                                    user_id,
                                    order_id,
                                    date_format(using_time, 'yyyy-MM-dd') date_id,
                                    using_time
                            FROM ${APP}.ods_coupon_use_inc
                            WHERE dt <= '${do_date}'
                              AND using_time IS NOT NULL;"


dwd_tool_coupon_pay_full="INSERT OVERWRITE TABLE ${APP}.dwd_tool_coupon_pay_inc PARTITION(dt='${do_date}')
                          SELECT
                                id,
                                coupon_id,
                                user_id,
                                order_id,
                                date_format(used_time,'yyyy-MM-dd') date_id,
                                used_time
                          FROM ${APP}.ods_coupon_use_inc
                          WHERE dt <= '${do_date}'
                            AND used_time IS NOT NULL;"


dwd_interaction_favor_add_full="INSERT OVERWRITE TABLE ${APP}.dwd_interaction_favor_add_inc PARTITION (dt = '${do_date}')
                                SELECT id
                                        , user_id
                                        , sku_id
                                        , date_format(create_time, 'yyyy-MM-dd') date_id
                                        , create_time
                                FROM ${APP}.ods_favor_info_inc
                                WHERE dt <= '${do_date}';"

dwd_interaction_comment_inc="
SET datanucleus.schema.autoCreateTables=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
WITH
	ci AS (
	      SELECT
		      id,
		      user_id,
		      sku_id,
		      order_id,
		      create_time,
		      appraise,
		      dt
	      from ${APP}.ods_comment_info_inc
	      where dt='${do_date}'
	      ),
	dic AS (
	      SELECT
		      dic_code,
		      dic_name
	      from ${APP}.ods_bash_dic_full
	      where parent_code='12'
	      )
INSERT OVERWRITE TABLE ${APP}.dwd_interaction_comment_inc partition(dt)
SELECT
	ci.id,
	ci.user_id,
	ci.sku_id,
	ci.order_id,
	date_format(ci.create_time,'yyyy-MM-dd') date_id,
	ci.create_time,
	ci.appraise,
	dic.dic_name,
	dt
from dic
     LEFT JOIN ci on ci.appraise=dic.dic_code;"

queries=(
    "$dwd_trade_cart_add_full"
    "$dwd_trade_order_detail_full"
    "$dwd_trade_pay_detail_suc_full"
    "$dwd_trade_cart_full"
    "$dwd_tool_coupon_get_full"
    "$dwd_tool_coupon_order_full"
    "$dwd_tool_coupon_pay_full"
    "$dwd_interaction_favor_add_full"
    "$dwd_interaction_comment_inc"
)


case $1 in
"dwd_trade_cart_add")
    hive -e "${queries[0]}"
;;
"dwd_trade_order_detail")
    hive -e "${queries[1]}"
;;
"dwd_trade_pay_detail_suc")
    hive -e "${queries[2]}"
;;
"dwd_trade_cart")
    hive -e "${queries[3]}"
;;
"dwd_tool_coupon_get")
    hive -e "${queries[4]}"
;;
"dwd_tool_coupon_order")
    hive -e "${queries[5]}"
;;
"dwd_tool_coupon_pay")
    hive -e "${queries[6]}"
;;
"dwd_interaction_favor_add")
    hive -e "${queries[7]}"
;;
"dwd_interaction_comment")
    hive -e "${queries[8]}"
;;
"all")
    for query in "${queries[@]}"; do
        hive -e "$query"
    done
;;
esac





# 单个测试

#  ods_to_dwd_init.sh dwd_trade_cart_add '2024-06-18'
#  ods_to_dwd_init.sh dwd_trade_cart_add '2024-06-18'
#  ods_to_dwd_init.sh dwd_trade_order_detail '2024-06-18'
#  ods_to_dwd_init.sh dwd_trade_pay_detail_suc '2024-06-18'
#  ods_to_dwd_init.sh dwd_tool_coupon_get '2024-06-18'
#  ods_to_dwd_init.sh dwd_tool_coupon_order '2024-06-18'
#  ods_to_dwd_init.sh dwd_tool_coupon_pay '2024-06-18'
#  ods_to_dwd_init.sh dwd_interaction_favor_add '2024-06-18'
#  ods_to_dwd_init.sh dwd_interaction_comment '2024-06-18'