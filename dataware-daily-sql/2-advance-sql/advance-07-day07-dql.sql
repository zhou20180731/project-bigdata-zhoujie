
USE hive_sql_zg6;
SHOW TABLES IN hive_sql_zg6;


-- todo: 1）、按年度列出每个商品销售总额
/*
从订单明细表（order_detail）中列出每个商品每个年度的购买总额。
*/



-- todo: 2）、某周内每件商品每天销售情况
/*
从订单详情表（order_detail）中查询2021年9月27号-2021年10月3号这一周所有商品每天销售情况。
*/


-- todo: 3）、查看每件商品的售价涨幅情况
/*
从商品价格变更明细表（sku_price_modify_detail），得到最近一次价格的涨幅情况，并按照涨幅升序排序。
*/


-- todo: 4）、销售订单首购和次购分析
/*
通过商品信息表（sku_info）订单信息表（order_info）订单明细表（order_detail）分析
如果有一个用户成功下单两个及两个以上的购买成功的手机订单（购买商品为xiaomi 10，apple 12，小米13）
那么输出这个用户的id及第一次成功购买手机的日期和第二次成功购买手机的日期，以及购买手机成功的次数。
*/




-- todo: 5）、同期商品售卖分析表
/*
从订单明细表（order_detail）中，求出同一个商品在2020年和2021年中同一个月的售卖情况对比。
*/



