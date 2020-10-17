import pandas as pd
import time
from sqlalchemy import create_engine

channel_use_col=['中心编码','中心名称','卖方商务中心编码','卖方商务中心',
                 '卖方合作模式大类(CRM)/一级分类(CMDM)','卖方合作模式小类(CRM)/二级分类(CMDM)',
                 '卖方客户编码','卖方客户名称','是否有效客户','渠道层级','单据日期','出库确认日期',
                 '买方客户编码','买方客户名称','买方合作模式大类(CRM)/一级分类(CMDM)',
                 '品类','产品线','仓库编码','仓库名称','营销小类','商品编码','商品名称',
                 '销售型号','门店编码','门店名称','门店一级分类','门店二级分类','开单数量',
                 '出库确认数量','含税价(折后)','开单金额(折前)','签收时间']

mmp_use_col=['商品型号','商品编码','大类','小类','门店编码','门店名称','门店等级',
             '门店一级分类','门店二级分类','导购员编码','导购员名称','导购员手机号',
             '导购类型','数量','单价','零售价','总价','资源抵扣金额','厂家承担券',
             '分部名称','CMDM中心编码','产品线','创建时间']


sample_use_col=['导购编码','导购员姓名','导购员手机','分部','门店编码',
                '门店名称','门店一级分类','门店二级分类','门店等级','代理商编码',
                '代理商名称','上样时间','商品大类','主体','型号编码','产品线',
                '型号','智能属性','智能体验情况','智能体验设备','样机条码',
                '类型','样机分类','是否竞品','门体数量','品牌']

engine=create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

print ("二级渠道数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
channel_file = r'E:\Share\每日导数\二级1017.xlsx'
channel_df = pd.read_excel(channel_file,sheet_name='   渠道出库明细',header=1,usecols=channel_use_col)
channel_df.to_sql('二级代理渠道零售数据', con=engine, if_exists='append', index=False)
print("二级渠道数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

print ("一级渠道数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
channel_file = r'E:\Share\每日导数\一级1017.xlsx'
channel_df = pd.read_excel(channel_file,sheet_name='   渠道出库明细',header=1,usecols=channel_use_col)
channel_df.to_sql('一级代理渠道零售数据', con=engine, if_exists='append', index=False)
print("一级渠道数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

print("mmp零售数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
mmp_file = r'E:\Share\每日导数\mmp1017.xlsx'
mmp_df = pd.read_excel(mmp_file,sheet_name='Sheet0',header=0,usecols=mmp_use_col)
mmp_df.to_sql('mmp零售数据', con=engine, if_exists='append', index=False)
print("mmp零售数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
