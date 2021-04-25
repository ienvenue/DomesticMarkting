# -*- coding: utf-8 -*- #
# ------------------------------------------------------------------
# File Name:        importdata.py
# Author:           yancy
# Version:          ver0.2
# Created:          2021/04/22
# Description:      Main Function:    XLSX或CSV文件导入MySQL数据库
# ------------------------------------------------------------------

import time
import pandas as pd
from sqlalchemy import create_engine


def file2db(filetype, path, tablename, sheetname, updatetype, specifycols, rownum, schema):
    """
    将xlsx文件导入到制定导入到数据库中，输出导入导出时间
    :param filetype: 指定文件类型,如xlsx、csv
    :param path: 指定文件地址
    :param tablename: 指定数据库表名
    :param sheetname: 指定Sheet页名
    :param updatetype: 指定导入方式，
                 *替换原表：repalce
                 *原表上追加：append
    :param specifycols: 指定导入的列，
                 *如果为None，则解析所有列。
                 *如果为str，则表示Excel上方字母所指定列或者列的范围（例如“A：E”或”A，C，E：F”），包含两端。
                 *如果为int-list，则表示要解析的列号列表（例如[0,1,2,3]）。
                 *如果为str-list，则表示要解析的列名称列表（例如["地市","中心"]）。
    :param rownum:指定从第rownum+1行开始导入，int类型
    :param schema:指定数据库schama、如ods、dim，str类型
    :return:无
    """
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if filetype == 'xlsx':
        df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=specifycols)
        df.to_sql(name=tablename, con=engine, if_exists=updatetype, index=False, schema=schema)
    elif filetype == 'csv':
        df = pd.read_csv(path, header=rownum, encoding="utf8", usecols=specifycols)
        df.to_sql(name=tablename, con=engine, if_exists=updatetype, index=False, schema=schema)
    else:
        print("导入文件类型错误,时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


# 主函数入口
if __name__ == '__main__':
    # 定义数据库连接
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306")
    # 参数说明：
    # filetype: 文件类型
    # schema:指定数据库schama、如ods、dim
    # path: 文件地址
    # tablename: 数据库表名
    # sheetname: Sheet页名(csv可填None)
    # updatetype: 'replace'(替换),'append'(追加)
    # specifycols: 指定导入的列，使用详见函数注释
    # rownum:指定从第rownum+1行开始导入
    file2db(filetype='xlsx',
            schema='dim',
            path=r'\\10.157.2.94\共享文件\奥维数据\配置表\奥维地市对应新中心.xlsx',
            tablename='奥维地市对应中心',
            sheetname='奥维',
            updatetype='replace',
            specifycols=None,
            rownum=0)
