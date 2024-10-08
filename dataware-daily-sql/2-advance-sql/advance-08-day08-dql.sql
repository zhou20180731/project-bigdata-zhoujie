
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、国庆期间每个品类的商品的收藏量和购买量
/*
从订单明细表（order_detail）和收藏信息表（favor_info）统计2021国庆期间，每个商品总收藏量和购买量。
*/



-- todo: 2）、统计活跃间隔对用户分级结果
/*
用户等级：
	忠实用户：近7天活跃且非新用户
	新晋用户：近7天新增
	沉睡用户：近7天未活跃但是在7天前活跃
	流失用户：近30天未活跃但是在30天前活跃
假设今天是数据中所有日期的最大值，从用户登录明细表中的用户登录时间给各用户分级，求出各等级用户的人数。
*/


-- todo: 3）、连续签到领金币数
/*
	用户每天签到可以领1金币，并可以累计签到天数，连续签到的第3、7天分别可以额外领2和6金币。
	每连续签到7天重新累积签到天数。
	从用户登录明细表中求出每个用户金币总数，并按照金币总数倒序排序。
*/


-- todo: 4）、国庆期间的7日动销率和滞销率
/*
动销率定义为品类商品中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数）。
滞销率定义为品类商品中一段时间内没有销量的商品占当前已上架总商品数的比例。（没有销量的商品 / 已上架总商品数）。
只要当天任一店铺有任何商品的销量就输出该天的结果。
从订单明细表（order_detail）和商品信息表（sku_info）表中求出国庆7天每天每个品类的商品的动销率和滞销率。
*/


-- todo: 5）、同时在线最多的人数
/*
根据用户登录明细表（user_login_detail），求出平台同时在线最多的人数。
*/




