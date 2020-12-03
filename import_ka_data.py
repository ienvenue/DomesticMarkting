import time
import re
import pandas as pd
from sqlalchemy import create_engine

def remodel(ser_str):
    """
    使用正则表达式替换【商品 - 长文本】的汉字，替换为空
    :param ser_str: 【商品 - 长文本】
    :return: 替换好的【商品型号】
    """
    pattern = re.compile('[\u4e00-\u9fa5]{0,}')
    return re.sub(pattern, "", ser_str)

def inventory2db(path, tablename, sheetname, type, cols, rownum):
    """
    读出excel数据，并按如下清洗数据
    1.替换商品型号中的汉字
    2.重命名列名
    :param path:excel文件路径
    :param tablename:数据库表名
    :param sheetname:Sheet页名
    :param type:导入类型 replace or append
    :param cols:字段名
    :param rownum:开始行数
    :return:无
    """
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    # 库存需要注释
    # df = df[(df['23.销售数量']!=0)]
    # 替换商品型号中的汉字
    df['商品型号']=df['商品 - 长文本'].map(remodel)
    df=df.drop('商品 - 长文本',axis=1)
    # 重命名列名
    df.rename(columns={"日历天": "时间", "商品 - 键值": "商品编码", "23.销售数量": "销量", "销售净额": "销额","3 销售净额（含促销补差）": "销额",
                       "库位标识（正品、残次、样机及赠品）":"库位标识"}, inplace=True)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)

def retail2db(path, tablename, sheetname, type, cols, rownum):
    """
    读出excel数据，并按如下清洗数据
    0.过滤销售量为0的垃圾数据
    1.替换商品型号中的汉字
    2.重命名列名
    :param path:excel文件路径
    :param tablename:数据库表名
    :param sheetname:Sheet页名
    :param type:导入类型 replace or append
    :param cols:字段名
    :param rownum:开始行数
    :return:无
    """
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    # 库存需要注释
    df = df[(df['23.销售数量']!=0)]
    # 替换商品型号中的汉字
    df['商品型号']=df['商品 - 长文本'].map(remodel)
    df=df.drop('商品 - 长文本',axis=1)
    # 重命名列名
    df.rename(columns={"日历天": "时间", "商品 - 键值": "商品编码", "23.销售数量": "销量", "销售净额": "销额","3 销售净额（含促销补差）": "销额",
                       "库位标识（正品、残次、样机及赠品）":"库位标识"}, inplace=True)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)

def csv2db(path, tablename, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_csv(path, header=rownum, usecols=cols, encoding="gbk")
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))





if __name__ == '__main__':
    # 国美Excel列
    GOME_2020_col = ['月份', '分部', '品牌', '日历天', '商品 - 键值', '商品 - 长文本', '23.销售数量', '3 销售净额（含促销补差）']
    GOME_2019_col = ['月份', '分部', '品牌', '日历天', '商品 - 键值', '商品 - 长文本', '23.销售数量', '销售净额']
    GOME_inventory_col=['分部','一二级分部','供应商','商品 - 长文本','商品 - 键值','库位标识（正品、残次、样机及赠品）','批次创建时间','采购类型','库存数量','含税库存金额']
    # 正式环境
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

    # 测试环境
    # engine=create_engine("mysql+pymysql://test:test123@10.157.6.74:3306/ods")

    # 导入数据
    # path_xlsx = r'E:\data\2020.xlsx'
    # path_xlsx1 = r'E:\data\1-4.xlsx'
    # path_xlsx2 = r'E:\data\6-8.xlsx'
    # path_xlsx3 = r'E:\data\9-12.xlsx'
    # path_xlsx4 = r'E:\data\5.xlsx'
    path_xlsx = r'E:\Share\每日导数\国美库存1123.xlsx'
    # print("导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # retail2db(path_xlsx, tablename='国美零售明细', sheetname='7-11', type='append', cols=GOME_2020_col,rownum=0)
    # retail2db(path_xlsx1, tablename='国美零售明细', sheetname='1-4', type='append', cols=GOME_2019_col,rownum=0)
    # retail2db(path_xlsx1, tablename='国美零售明细', sheetname='1-4', type='append', cols=GOME_2019_col,rownum=0)
    # retail2db(path_xlsx2, tablename='国美零售明细', sheetname='6-7', type='append', cols=GOME_2019_col,rownum=0)
    # retail2db(path_xlsx2, tablename='国美零售明细', sheetname='8', type='append', cols=GOME_2019_col,rownum=0)
    # retail2db(path_xlsx3, tablename='国美零售明细', sheetname='9-12', type='append', cols=GOME_2019_col,rownum=0)
    # retail2db(path_xlsx4, tablename='国美零售明细', sheetname='5', type='append', cols=GOME_2019_col,rownum=0)
    inventory2db(path_xlsx, tablename='国美库存明细', sheetname='整体', type='replace', cols=GOME_inventory_col,rownum=0)
    # print("导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))