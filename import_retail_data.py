import pandas as pd
import pymysql
from sqlalchemy import create_engine


# 1、读取df
# 2、选择df时间段
# 3、删除数据库中对应时间段数据

def load_data(file_path, sheet_name,rownum,usecol):
    """
    读取数据
    :param file_path: 文件路径
    :param sheet_name: sheet名
    :return: dataframe
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name,header=rownum,usecols=usecol)
    return df

def del_data(df, tablename):
    """
    删除数据库中待导入数据对应日期的数据
    :param df: 待导入的dataframe
    :param tablename: 待导入的数据库表名
    :return: 不返回值，只删除对应时间的数据
    """
    conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.')
    cur = conn.cursor()
    if tablename == 'mmp零售数据全量':
        col = '创建时间'
    else:
        col = '出库确认日期'
    end_date = df[col].max()
    start_date = df[col].min()
    sql = '''
        delete from ods.{} where {} between '{}' and '{}' 
        '''.format(tablename, col, start_date, end_date)
    print(sql)
    cur.execute(sql)
    cur.close()
    conn.commit()
    conn.close()

def save_date(df, tablename,type):
    """
    df存储到数据库
    :param df: 待导入的数据
    :param tablename: 待导入的数据库表名
    :return: 不返回值，数据存储到数据库
    """
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")
    df.to_sql(tablename, con=engine, index=False, if_exists=type)

if __name__ == '__main__':
    mmp_use_col = ['上报ID', '商品型号', '商品编码', '大类', '小类', '门店编码', '门店名称', '门店等级',
                   '门店一级分类', '门店二级分类', '导购员编码', '导购员名称', '导购员手机号',
                   '导购类型', '数量', '单价', '零售价', '总价', '资源抵扣金额', '厂家承担券',
                   '分部名称', 'CMDM中心编码', '产品线', '创建时间', '顾客手机']

    df = pd.read_excel(r'D:\MyData\zhangmd10\Desktop\新建文件夹 (2)\mmp11.2.xlsx', encoding='gbk', usecols=mmp_use_col)
    del_data(df, 'mmp零售数据全量')
