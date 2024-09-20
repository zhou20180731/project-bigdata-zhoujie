#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi

#1 交易域-加购-事务事实表
dwd_trade_cart_add_inc="WITH cart_info AS
                       	     (
                       	     SELECT data.id,
                       	            data.user_id,
                       	            data.sku_id,
                       	            date_format(from_utc_timestamp(data.dt * 1000, 'GMT+8'), 'yyyy-MM-dd') date_id,
                       	            date_format(from_utc_timestamp(data.dt * 1000, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') create_time,
                       	            data.source_id,
                       	            data.source_type source_type_code,
                       	            IF(data.source_type = 'insert', data.sku_num, 0) sku_num
                       	     FROM ${APP}.ods_cart_info_full data
                       	     WHERE dt = '${do_date}'
                       	     ),
                            base_dic AS
                       	     (
                       	     SELECT dic_code,
                       	            dic_name source_type_name
                       	     FROM ${APP}.ods_base_dic_full
                       	     WHERE dt = '${do_date}'
                       		   AND parent_code = '24'
                       	     )
                       INSERT OVERWRITE TABLE ${APP}.dwd_trade_cart_add_inc PARTITION (dt = '${do_date}')
                       SELECT id,
                              user_id,
                              sku_id,
                              date_id,
                              create_time,
                              source_id,
                              source_type_code,
                              source_type_name,
                              sku_num
                       FROM cart_info cart
                            LEFT JOIN base_dic dic ON cart.source_type_code = dic.dic_code;"


#2 交易域-下单-事务事实表
dwd_trade_order_detail_inc="WITH
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
	          WHERE dt = '${do_date}'
	          ),
	info AS (
	          SELECT id,
	                 user_id,
	                 province_id
	          FROM ${APP}.ods_order_info_inc
	          WHERE dt = '${do_date}'
	          ),
	activity AS (
	          SELECT order_detail_id,
	                 activity_id,
	                 activity_rule_id
	          FROM ${APP}.ods_order_detail_activity_inc
	          WHERE dt = '${do_date}'
	          ),
	coupon AS (
	          SELECT order_detail_id,
	                 coupon_id
	          FROM ${APP}.ods_order_detail_coupon_inc
	          WHERE dt = '${do_date}'
	          ),
	dic AS (
	          SELECT dic_code,
	                 dic_name
	          FROM ${APP}.ods_base_dic_full
	          WHERE dt = '${do_date}'
	          )
INSERT
OVERWRITE
TABLE
${APP}.dwd_trade_order_detail_inc
PARTITION(dt = '${do_date}')
SELECT
       info.id,
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
     LEFT JOIN dic ON detail.source_type = dic.dic_code"

#3. 交易域-支付成功-事务事实表
dwd_trade_pay_detail_suc_inc="
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
    WHERE (dt = '${do_date}' or dt = date_add('${do_date}',-1)))
      ,pi AS (SELECT
                    user_id,
                    order_id,
                    payment_type,
                    callback_time
                    FROM ${APP}.ods_payment_info_inc
                    WHERE dt='${do_date}'
                      AND payment_status='1602')
      ,oi AS (
              SELECT
                id,
                province_id
              FROM ${APP}.ods_order_info_inc
              WHERE (dt = '${do_date}' or dt = date_add('${do_date}',-1)))
      ,act AS (
              SELECT
                order_detail_id,
                activity_id,
                activity_rule_id
              FROM ${APP}.ods_order_detail_activity_inc
              WHERE (dt = '${do_date}' or dt = date_add('${do_date}',-1)))
      ,cou AS (
              SELECT
                order_detail_id,
                coupon_id
              FROM ${APP}.ods_order_detail_coupon_inc
              WHERE (dt = '${do_date}' or dt = date_add('${do_date}',-1)))
      ,pay_dic AS (
                  SELECT
                    dic_code,
                    dic_name
                  FROM ${APP}.ods_base_dic_full
                  WHERE dt='${do_date}'
                    AND parent_code='11')
      ,src_dic AS (
                  SELECT
                    dic_code,
                    dic_name
                  FROM ${APP}.ods_base_dic_full
                  WHERE dt='${do_date}'
        and parent_code='24'
                                                                          )
    INSERT
    OVERWRITE
    TABLE
    ${APP}.dwd_trade_pay_detail_suc_inc
    PARTITION
    (
    dt = '${do_date}'
    )
    SELECT
          od.id,
          od.order_id,
          pi.user_id,
          sku_id,
          province_id,
          activity_id,
          activity_rule_id,
          coupon_id,
          pi.payment_type,
          pay_dic.dic_name,
          date_format(pi.callback_time,'yyyy-MM-dd') date_id,
          pi.callback_time,
          source_id,
          source_type,
          src_dic.dic_name,
          sku_num,
          split_original_amount,
          split_activity_amount,
          split_coupon_amount,
          split_total_amount
    FROM od
        JOIN pi ON od.order_id=pi.order_id
        LEFT JOIN oi ON od.order_id=oi.id
        LEFT JOIN act ON od.id=act.order_detail_id
        LEFT JOIN cou ON od.id=cou.order_detail_id
        LEFT JOIN pay_dic ON pi.payment_type=pay_dic.dic_code
        LEFT JOIN src_dic ON od.source_type=src_dic.dic_code;"


#5 工具域-优惠券领取-事务事实表
dwd_tool_coupon_get_inc="
    INSERT OVERWRITE TABLE ${APP}.dwd_tool_coupon_get_inc partition (dt = '${do_date}')
    SELECT id
          , coupon_id
          , user_id
          , date_format(get_time, 'yyyy-MM-dd') date_id
          , get_time
    FROM ${APP}.ods_coupon_use_inc
    WHERE dt = '${do_date}'"



#6 工具域-优惠券使用(下单)-事务事实表
dwd_tool_coupon_order_inc="
    INSERT OVERWRITE table ${APP}.dwd_tool_coupon_order_inc PARTITION (dt = '${do_date}')
    SELECT id,
            coupon_id,
            user_id,
            order_id,
            date_format(using_time, 'yyyy-MM-dd') date_id,
            using_time
    FROM ${APP}.ods_coupon_use_inc
    WHERE dt = '${do_date}'
      AND using_time IS NOT NULL"


#7 工具域-优惠券使用(支付)-事务事实表
dwd_tool_coupon_pay_inc="
    INSERT OVERWRITE table ${APP}.dwd_tool_coupon_pay_inc PARTITION(dt='${do_date}')
    SELECT
          id,
          coupon_id,
          user_id,
          order_id,
          date_format(used_time,'yyyy-MM-dd') date_id,
          used_time
    FROM ${APP}.ods_coupon_use_inc
    WHERE dt='${do_date}'
      AND used_time is not null"

#8 互动域-收藏商品-事务事实表
dwd_interaction_favor_add_inc="
    INSERT OVERWRITE table ${APP}.dwd_interaction_favor_add_inc PARTITION (dt = '${do_date}')
    SELECT id
            , user_id
            , sku_id
            , date_format(create_time, 'yyyy-MM-dd') date_id
            , create_time
    FROM ${APP}.ods_favor_info_inc
    WHERE dt = '${do_date}'"

# 9 互动域-评价-事务事实表
dwd_interaction_comment_inc="
    WITH
      ci AS (
            select
                  id,
                  user_id,
                  sku_id,
                  order_id,
                  create_time,
                  appraise
            from ${APP}.ods_comment_info_inc
            where dt='${do_date}'
            ),
      dic AS (
            select
                  dic_code,
                  dic_name
            from ${APP}.ods_base_dic_full
            where dt='${do_date}'
            and parent_code='12'
            )
    insert overwrite table ${APP}.dwd_interaction_comment_inc partition(dt='${do_date}')
    select
          ci.id,
          ci.user_id,
          ci.sku_id,
          ci.order_id,
          date_format(ci.create_time,'yyyy-MM-dd') date_id,
          ci.create_time,
          ci.appraise,
          dic.dic_name
    from dic
        LEFT JOIN ci on ci.appraise=dic.dic_code;
    -- 测试

    SHOW PARTITIONS gmall.dwd_interaction_comment_inc;
    SELECT * FROM gmall.dwd_interaction_comment_inc WHERE dt = '2024-06-18' LIMIT 10
"

queries=(
    "$dwd_trade_cart_add_inc"
    "$dwd_trade_order_detail_inc"
    "$dwd_trade_pay_detail_suc_inc"
    "$dwd_tool_coupon_get_inc"
    "$dwd_tool_coupon_order_inc"
    "$dwd_tool_coupon_pay_inc"
    "$dwd_interaction_favor_add_inc"
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
"dwd_tool_coupon_get")
    hive -e "${queries[3]}"
;;
"dwd_tool_coupon_order")
    hive -e "${queries[4]}"
;;
"dwd_tool_coupon_pay")
    hive -e "${queries[5]}"
;;
"dwd_interaction_favor_add")
    hive -e "${queries[6]}"
;;
"dwd_interaction_comment")
    hive -e "${queries[7]}"
;;
"all")
    for query in "${queries[@]}"; do
        hive -e "$query"
    done
;;
esac




#chmod u+x ods_to_dwd.sh

#sh ods_to_dwd_init.sh all '2024-06-19'






