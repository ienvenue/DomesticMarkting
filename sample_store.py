import pymysql
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

mmp_use_col = ['商品型号', '商品编码', '大类', '小类', '门店编码', '门店名称', '门店等级',
               '门店一级分类', '门店二级分类', '导购员编码', '导购员名称', '导购员手机号',
               '导购类型', '数量', '单价', '零售价', '总价', '资源抵扣金额',
               '分部名称', 'CMDM中心编码', '产品线', '创建时间']

channel_use_col = ['中心编码', '中心名称', '卖方商务中心编码', '卖方商务中心',
                   '卖方合作模式大类(CRM)/一级分类(CMDM)', '卖方合作模式小类(CRM)/二级分类(CMDM)',
                   '卖方客户编码', '卖方客户名称', '是否有效客户', '渠道层级', '单据日期', '出库确认日期',
                   '买方客户编码', '买方客户名称', '买方合作模式大类(CRM)/一级分类(CMDM)',
                   '品类', '产品线', '仓库编码', '仓库名称', '营销小类', '商品编码', '商品名称',
                   '销售型号', '门店编码', '门店名称', '门店一级分类', '门店二级分类', '开单数量',
                   '出库确认数量', '含税价(折后)', '开单金额(折前)', '签收时间']

sample_use_col = ['导购编码', '导购员姓名', '导购员手机', '分部', '门店编码',
                  '门店名称', '门店一级分类', '门店二级分类', '门店等级', '代理商编码',
                  '代理商名称', '上样时间', '商品大类', '主体', '型号编码', '产品线',
                  '型号', '智能属性', '智能体验情况', '智能体验设备', '样机条码',
                  '类型', '样机分类', '是否竞品', '门体数量', '品牌']

store_use_col = ['门店名称', '门店编码', '千店导购', '虚拟门店', '一级分类', '二级分类', '经营状态',
                 '所属事业部', '事业部编码', '所属分部', '中心编码', '经营单位类型', '经营单位编码',
                 '经营单位名称', '事业部分类', '分销商供货方名称', '所属代理商编码', '所属代理商名称',
                 '产品线名称', '省', '市', '县/区', '镇', '镇/街道编码', '详细地址', '集团运营中心',
                 '客户门店编码', '客户门店名称', '网点分级', '门店等级', '市场等级', '创建时间',
                 '更新时间', 'TERMINAL_ORG_ID', 'TERMINAL_ID']

# 正式环境
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")


# 测试环境
# engine=create_engine("mysql+pymysql://test:test123@10.157.6.74:3306/ods")

def excel2db(path, tablename, sheetname, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    try:
        df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
        df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    except Exception as e:
        print('导入失败')
        return (e)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def csv2db(path, tablename, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_csv(path, header=rownum, usecols=cols, encoding="gbk")
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


# df=pd.read_excel('C:\Users\ex_chenyj12\Downloads\出样规则配置表.xlsx', sheet_name='Sheet0', header=0)

# excel2db(path=r'E:\Share\每日导数\样机上样20201014.xlsx',\
#          tablename='样机上样数据',sheetname='Sheet0',type='replace',cols=sample_use_col,rownum=0)

# csv2db(path=r'C:\Users\ex_chenyj12\Documents\线下干衣机月度永久表.csv',\
#          tablename='线下干衣机月度永久表',type='replace',cols=None,rownum=0)
#
excel2db(path=r'E:\Share\每日导数\上样1020.xlsx', tablename='样机上样数据', sheetname='Sheet0', type='replace', cols=sample_use_col,
         rownum=0)
