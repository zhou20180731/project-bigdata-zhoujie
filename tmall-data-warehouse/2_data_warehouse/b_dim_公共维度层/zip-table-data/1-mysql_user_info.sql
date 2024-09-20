

-- 创建数据库
CREATE DATABASE IF NOT EXISTS tmall_tmp ;
USE tmall_tmp;

-- 创建表
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '编号',
    `login_name` varchar(200) DEFAULT NULL COMMENT '用户名称',
    `nick_name` varchar(200) DEFAULT NULL COMMENT '用户昵称',
    `passwd` varchar(200) DEFAULT NULL COMMENT '用户密码',
    `name` varchar(200) DEFAULT NULL COMMENT '用户姓名',
    `phone_num` varchar(200) DEFAULT NULL COMMENT '手机号',
    `email` varchar(200) DEFAULT NULL COMMENT '邮箱',
    `head_img` varchar(200) DEFAULT NULL COMMENT '头像',
    `user_level` varchar(200) DEFAULT NULL COMMENT '用户级别',
    `birthday` date DEFAULT NULL COMMENT '用户生日',
    `gender` varchar(1) DEFAULT NULL COMMENT '性别 M男,F女',
    `create_time` datetime DEFAULT NULL COMMENT '创建时间',
    `operate_time` datetime DEFAULT NULL COMMENT '修改时间',
    `status` varchar(200) DEFAULT NULL COMMENT '状态',
    PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1012 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- 插入数据：2024-06-18
INSERT INTO `tmall_tmp`.`user_info` VALUES ('1', 'g5vuvt', '阿元', null, '茅钧', '13267245671', 'g5vuvt@aol.com', null, '1', '1972-05-17', 'M', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('2', '1zxcfajbop5', '芳燕', null, '和芳燕', '13165513595', '1zxcfajbop5@163.com', null, '1', '2001-07-17', 'F', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('3', '6ob7y5v7y0', '阿岩', null, '公孙岩', '13474673448', '6ob7y5v7y0@0355.net', null, '2', '2007-07-17', null, '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('4', 'ngkwm6q640', '春菊', null, '王芝', '13977633429', 'ngkwm6q640@aol.com', null, '1', '2008-05-17', 'F', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('5', '9eggpruv', '勇毅', null, '鲍泽晨', '13851362474', '9eggpruv@gmail.com', null, '1', '2007-04-17', 'M', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('6', '1ouocgjxa', '菁梦', null, '孟馨艺', '13863932811', '1ouocgjxa@sina.com', null, '1', '1997-02-17', null, '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('7', '6mya9h6qyanu', '政谦', null, '吴航弘', '13955343482', '6mya9h6qyanu@gmail.com', null, '1', '1983-10-17', 'M', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('8', '85sdnc3yi9q', '芸芸', null, '彭爱', '13779723342', 's4tf0grvkzhp@yeah.net', null, '1', '1987-10-17', 'F', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('9', '9pvt9lwq206s', '行时', null, '康行时', '13556126278', '9pvt9lwq206s@ask.com', null, '1', '2005-11-17', null, '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);
INSERT INTO `tmall_tmp`.`user_info` VALUES ('10', 'kimpfl47c8a', '露瑶', null, '葛姣婉', '13536199166', 'jlqk37@3721.net', null, '1', '2003-01-17', 'F', '2024-06-18 18:17:35', '2024-06-18 18:17:35', null);



-- 插入数据：2024-06-19
REPLACE INTO `tmall_tmp`.`user_info` VALUES ('9', '9pvt9lwq206s', '行时', null, '康行时', '13556126278', '9pvt9lwq206s@ask.com', null, '1', '2005-11-17', 'M', '2024-06-18 18:17:35', '2024-06-19 18:17:35', null);
REPLACE INTO `tmall_tmp`.`user_info` VALUES ('10', 'kimpfl47c8a', '露瑶', null, '葛姣婉', '13536199166', 'luyao@3721.net', null, '1', '2003-01-17', 'F', '2024-06-18 18:17:35', '2024-06-19 18:17:35', null);
REPLACE INTO `tmall_tmp`.`user_info` VALUES ('11', 'ddddsere', '马涛', null, '海涛', '13977633428', 'matao@126.com', null, '1', '2004-01-12', 'M', '2024-06-19 09:00:23', '2024-06-19 09:00:23', null);


select *from  user_info;
