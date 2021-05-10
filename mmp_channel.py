import pandas as pd
import time
from sqlalchemy import create_engine
import pymysql

# conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.', database='ods')
#
# cur = conn.cursor()
#
# del_sql_level1 = 'delete from ods.一级代理渠道零售数据 a where ods.当月月累(a.单据日期) '
# del_sql_level2 = 'delete from ods.二级代理渠道零售数据  a where ods.当月月累(a.单据日期) '
# # 每月删除一次
# # del_sql_mmp = 'delete from ods.mmp零售数据全量 a where ods.当月月累(a.创建时间) '
#
# print('一级删除{}行'.format(cur.execute(del_sql_level1)))
# print('二级删除{}行'.format(cur.execute(del_sql_level2)))
# # print('删除{}行'.format(cur.execute(del_sql_mmp)))
#
# conn.commit()
# conn.close()

channel_use_col = ['中心编码', '中心名称', '卖方商务中心编码', '卖方商务中心',
                   '卖方合作模式大类(CRM)/一级分类(CMDM)', '卖方合作模式小类(CRM)/二级分类(CMDM)',
                   '卖方客户编码', '卖方客户名称', '是否有效客户', '渠道层级', '单据日期', '出库确认日期',
                   '买方客户编码', '买方客户名称', '买方合作模式大类(CRM)/一级分类(CMDM)',
                   '品类', '产品线', '仓库编码', '仓库名称', '营销小类', '商品编码', '商品名称',
                   '销售型号', '门店编码', '门店名称', '门店一级分类', '门店二级分类', '开单数量',
                   '出库确认数量', '含税价(折后)', '开单金额(折前)', '签收时间']

mmp_use_col = ['订单分类','套餐编码','套餐名称','套餐数量','套餐单价','套餐抵扣金额','上报ID', '上报单号', '来源单号', '订单来源', '上报类型',
                '产品类型', '商品型号', '商品编码', '商品名称', '大类', '小类', '商品备注',
            '智能属性', '门店编码', '门店名称', '门店等级', '经营单位编码', '经营单位名称', '所属代理商编码', '所属代理商名称', '导购经销商编码',
            '导购经销商名称', '门店一级分类', '门店二级分类', '门店三级分类', '事业部分类', '导购员编码', '导购员名称', '导购员手机号', '导购类型',
            '数量', '单价', '零售价', '总价', '资源抵扣金额', '厂家承担券', '系统承担券', '券后金额',  '分部编码', '分部名称',
            'CMDM中心编码', '大区', '省份(门店)', '地级市(门店)', '县/区(门店)', '品牌',  '容量', '容量段',
            '产品定位', '品类', '产品线', '顾客固话', '是否手工导入', '顾客手机',
            '上报时间', '备注', '配件', '条码', '条码编码', '条码型号', '条码名称',
            '条码备注', '扫码类型', '与CSS是否重复', '与CCS是否重复', '是否补贴依据', '是否计算补贴', '返现金额', '带单金额',  '带单电话', '卡号', '所属区域',
            '验证码', 'OMS码', '代收货款', '配送时间', '安装时间', '配送方式', '来源系统', '是否有导购', '零售主管', '认筹券号', '美的通版本', '创建人', '创建时间',
            '是否美居用户', '激活属性', '激活补录手机号', '激活时间']

sample_use_col = ['导购编码', '导购员姓名', '导购员手机', '分部', '门店编码',
                  '门店名称', '门店一级分类', '门店二级分类', '门店等级', '代理商编码',
                  '代理商名称', '上样时间', '商品大类', '主体', '型号编码', '产品线',
                  '型号', '智能属性', '智能体验情况', '智能体验设备', '样机条码',
                  '类型', '样机分类', '是否竞品', '门体数量', '品牌']

engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

# print("二级渠道数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
# channel_file = r'E:\Share\每日导数\二级1114.xlsx'
# channel_df = pd.read_excel(channel_file, sheet_name='   渠道出库明细', header=1, usecols=channel_use_col)
# channel_df.to_sql('二级代理渠道零售数据', con=engine, if_exists='append', index=False)
# print("二级渠道数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '共插入{}行'.format(channel_df.shape[0]))
#
# print("一级渠道数据导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
# channel_file = r'E:\Share\每日导数\一级1114.xlsx'
# channel_df = pd.read_excel(channel_file, sheet_name='   渠道出库明细', header=1, usecols=channel_use_col)
# channel_df.to_sql('一级代理渠道零售数据', con=engine, if_exists='append', index=False)
# print("一级渠道数据导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '共插入{}行'.format(channel_df.shape[0]))

print("mmp零售数据全量导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
mmp_file = r'D:\MyData\ex_chenyj12\Desktop\洗衣机mmp5.1-5.5.xlsx'
mmp_df = pd.read_excel(mmp_file, sheet_name='Sheet0', header=0, usecols=mmp_use_col)
mmp_df.to_sql('mmp零售数据全量', con=engine, if_exists='append', index=False)
print("mmp零售数据全量导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '共插入{}行'.format(mmp_df.shape[0]))
