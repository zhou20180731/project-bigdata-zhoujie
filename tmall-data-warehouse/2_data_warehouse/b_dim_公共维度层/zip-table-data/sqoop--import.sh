
# ======================================================================
#              todo：【同步一次】 日期信息表：date_info
# ======================================================================
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/db_test \
--username root \
--password 123456 \
--target-dir /tmp_dim_date_info/ \
--delete-target-dir \
--query "SELECT
    date_id,
    week_id,
    week_day,
    day,
    month,
    quarter,
    year,
    is_workday,
    holiday_id
FROM date_info
WHERE \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--null-string '\\N' \
--null-non-string '\\N'



