# !/usr/bin/env/python
# -*- coding: utf-8 -*-


# 导入pymysql模块
import pymysql
import numpy as np


if __name__ == '__main__':
    """
        安装NumPy命令:
            pip install numpy
        pymysql 安装:
            pip install pymysql
    """
    print('Hello World')

    # 连接database
    conn = pymysql.connect(
        host="node101", user="root", password="123456", database="gmall"
    )

    # 获取游标
    cursor = conn.cursor()

    # 查询所有表名
    cursor.execute("SELECT TABLE_NAME, TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA = 'gmall'")
    tables = cursor.fetchall()

    # 遍历所有表
    for table in tables:
        # 获取表名
        table_name = table[0]
        table_comment = table[1]
        # 查询表结构
        cursor.execute(
            "SELECT column_name, data_type, COLUMN_COMMENT FROM information_schema.COLUMNS WHERE table_name = '{}' AND table_schema = 'gmall'".format(table_name)
        )
        columns = cursor.fetchall()
        # print(columns)
        array1 = np.array(columns)
        # print(len(array1))
        count = len(array1)
        field_trans_type = {'varchar': 'STRING', 'double': 'DOUBLE', 'char': 'STRING', 'float': 'DOUBLE', 'int': 'BIGINT',
                            'datetime': 'STRING', 'date': 'STRING', 'time': 'STRING', 'boolean': 'BOOLEAN', 'int32': 'INT',
                            'uint32': 'INT', 'decimal': 'DECIMAL(16, 2)', 'timestamp': 'STRING', 'bigint': 'BIGINT', 'string': 'STRING'}
        hive_drop_ddl = "DROP TABLE IF EXISTS ods_{} ;\n".format(table_name)
        hive_ddl = hive_drop_ddl + "CREATE EXTERNAL TABLE ods_{} (\n".format(table_name)
        for column in columns:
            column_name = column[0]
            column_type = column[1]
            column_comment = column[2]
            hive_ddl += "\t{} {} COMMENT '{}', \n".format(column_name, field_trans_type.get(column_type), column_comment)
        hive_ddl = hive_ddl[:-3] + """\n) 
COMMENT '{}'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '/warehouse/gmall/ods/{}';""".format(table_comment, table_name)
        print(hive_ddl)
        print('-- =====================================================')

    # 关闭游标和连接
    cursor.close()
    conn.close()

