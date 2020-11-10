import time
import pandas as pd
from sqlalchemy import create_engine

yuedu_xianshangganyiji = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式',
                          '品牌类型', '容量', '上市月度', '上市周度', '子品牌', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段']

yuedu_xianshangxiyiji = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸', '电机类型',
                         '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度', '洗涤类型', '智能',
                         '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段'
                         ]
zhoudu_xianshangxiyiji = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸',
                          '电机类型', '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度',
                          '洗涤类型', '智能', '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段']

zhoudu_xianshangganyiji = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式', '品牌类型', '容量',
                           '上市月度', '上市周度', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段', '子品牌'
                           ]
yuedu_xianxiaganyiji = ['销量', '销额', '单价', '月度', '品牌', '机型', '干衣方式', '上市月度', '上市周度', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                        '线下价格段', '线下容量段']

yuedu_xianxiaxiyiji = ['销量', '销额', '单价', '月度', '品牌', '机型', '上市月度', '上市周度', '洗涤类型', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                       '通用价格段新', '通用容量段新'
                       ]

# 正式环境
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/aowei")


# 测试环境
# engine=create_engine("mysql+pymysql://test:test123@10.157.6.74:3306/ods")

def excel2db(path, tablename, sheetname, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def aoweizhoudu2db(path, tablename, sheetname, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    df['年'] = df['周度'].map(lambda x: x.split('W')[0])
    df['周'] = df['周度'].map(lambda x: x.split('W')[1])
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def csv2db(path, tablename, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_csv(path, header=rownum, usecols=cols, encoding="gbk")
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


# excel2db(path=r'E:\Share\每日导数\样机上样20201014.xlsx',\
#          tablename='样机上样数据',sheetname='Sheet0',type='replace',cols=sample_use_col,rownum=0)
# csv2db(path=r'C:\Users\ex_chenyj12\Documents\线下干衣机月度永久表.csv',\
#          tablename='线下干衣机月度永久表',type='replace',cols=None,rownum=0)


if __name__ == '__main__':
    # aoweizhoudu2db(path=r'E:\Share\每日导数\线上洗衣机19年周度.xlsx', tablename='周度线上洗衣机', sheetname='源数据', type='replace',
    #                cols=zhoudu_xianshangxiyiji, rownum=0)
    # aoweizhoudu2db(path=r'E:\Share\每日导数\线上洗衣机20年周度.xlsx', tablename='周度线上洗衣机', sheetname='源数据', type='append',
    #                cols=zhoudu_xianshangxiyiji, rownum=0)
    # excel2db(path=r'E:\Share\每日导数\品牌变化配置表.xlsx', tablename='品牌变化配置表', sheetname='源数据', type='replace',
    #          cols=yuedu_xianxiaganyiji, rownum=0)
    # excel2db(path=r'E:\Share\每日导数\线下干衣机月度20.7-20.9.xlsx', tablename='月度线下干衣机', sheetname='源数据', type='append',
    #                cols=yuedu_xianshangganyiji, rownum=0)
    # df = pd.read_excel(r'E:\Share\每日导数\品牌变化配置表.xlsx', sheet_name='源数据', header=0)
    # df.to_sql(name='品牌变化配置表', con=engine, if_exists='replace', index=False)
    csv2db(path=r'E:\Share\每日导数\report.csv', tablename='线下干衣机月度永久表', type='replace',cols=None,rownum=0)