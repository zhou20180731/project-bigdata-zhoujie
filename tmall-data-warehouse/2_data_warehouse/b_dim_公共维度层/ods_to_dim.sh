#!/bin/bash

APP=gmall

if [ -n "$2" ] ;then
    do_date=$2
else
  echo "请传入日期参数"
  exit
fi

#6 用户维度表
dim_user_sql="
WITH
--     用户信息表
user_info AS (
    SELECT
        id,
        login_name,
        nick_name,
        passwd,
        name,
        phone_num,
        email,
        head_img,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        status,
        '${do_date}' AS start_date,
        '9999-12-31' AS end_date
    FROM gmall.ods_user_info_inc
    WHERE dt='${do_date}'
)

INSERT INTO TABLE gmall.dim_user_zip PARTITION (dt='${do_date}')
SELECT
    id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date
FROM user_info;
"

case $1 in
  "dim_user"){
    hive -e "${dim_user_sql}"
  };;
  "all"){
    hive -e "
    ${dim_user_sql};
    "
  };;
esac