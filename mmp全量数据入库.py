import pandas as pd
import time
from sqlalchemy import create_engine
import os


file_path = r'\\10.157.2.94\临时文件\基础数据'
os.chdir(file_path)
file_list = [file for file in os.listdir() if file.find('xlsx') != -1]

mmp_use_col=['上报ID','商品型号','商品编码','大类','小类','门店编码','门店名称','门店等级',
             '门店一级分类','门店二级分类','导购员编码','导购员名称','导购员手机号',
             '导购类型','数量','单价','零售价','总价','资源抵扣金额','厂家承担券',
             '分部名称','CMDM中心编码','产品线','创建时间','顾客手机']

engine=create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

for file in file_list:
    print("mmp零售数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    mmp_df = pd.read_excel(file, sheet_name='Sheet0', header=0, usecols=mmp_use_col)
    mmp_df.to_sql('mmp零售数据全量', con=engine, if_exists='append', index=False)
    print("mmp零售数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

print('存储完毕')