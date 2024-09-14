# ======================================================================
#                   todo：3. 用户表：user_info（增量，历史）
# ======================================================================
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/gmall \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/user_info_inc/2024-06-18 \
--delete-target-dir \
--query "SELECT
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
    status
FROM user_info
WHERE date_format(create_time, '%Y-%m-%d') <= '2024-06-18' AND \$CONDITIONS" \
--num-mappers 1 \
--split-by 'id' \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'



# ======================================================================
#                   todo：3.用户表：user_info（增量，每日）
# ======================================================================
/opt/module/sqoop/bin/sqoop import \
--connect jdbc:mysql://node101:3306/gmall \
--username root \
--password 123456 \
--target-dir /origin_data/gmall/user_info_inc/2024-06-19 \
--delete-target-dir \
--query "SELECT
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
    status
FROM user_info
WHERE date_format(create_time, '%Y-%m-%d') = '2024-06-19' and \$CONDITIONS" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec gzip \
--null-string '\\N' \
--null-non-string '\\N'



#CREATE TABLE `user_info` (
#  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
#  `login_name` varchar(200) DEFAULT NULL COMMENT '用户名称',
#  `nick_name` varchar(200) DEFAULT NULL COMMENT '用户昵称',
#  `passwd` varchar(200) DEFAULT NULL COMMENT '用户密码',
#  `name` varchar(200) DEFAULT NULL COMMENT '用户姓名',
#  `phone_num` varchar(200) DEFAULT NULL COMMENT '手机号',
#  `email` varchar(200) DEFAULT NULL COMMENT '邮箱',
#  `head_img` varchar(200) DEFAULT NULL COMMENT '头像',
#  `user_level` varchar(200) DEFAULT NULL COMMENT '用户级别',
#  `birthday` date DEFAULT NULL COMMENT '用户生日',
#  `gender` varchar(1) DEFAULT NULL COMMENT '性别 M男,F女',
#  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
#  `operate_time` datetime DEFAULT NULL COMMENT '修改时间',
#  `status` varchar(200) DEFAULT NULL COMMENT '状态',
#  PRIMARY KEY (`id`) USING BTREE
#) ENGINE=InnoDB AUTO_INCREMENT=1601 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;