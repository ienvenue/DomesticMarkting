import pandas as pd
from sqlalchemy import create_engine

月度线上干衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式',
                      '品牌类型', '容量', '上市月度', '上市周度', '子品牌', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段']

月度线上洗衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸', '电机类型',
                       '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度', '洗涤类型', '智能',
                       '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段'
                       ]
周度线上洗衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸',
                      '电机类型', '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度',
                      '洗涤类型', '智能', '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段']

周度线上干衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式', '品牌类型', '容量',
                     '上市月度', '上市周度', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段', '子品牌'
                     ]
月度线下干衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '干衣方式', '上市月度', '上市周度', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                       '线下价格段', '线下容量段']

月度线下洗衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '上市月度', '上市周度', '洗涤类型', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                        '通用价格段新', '通用容量段新'
                        ]

周度线下洗衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '变频', '产品类型', '产品系列', '尺寸',
                       '电机类型', '耗电量', '烘干', '烘干类型', '厚度', '能效等级', '品牌类型', '容量', '上市月度', '上市周度',
                       '洗涤类型', '智能', '转速', '子品牌', '通用价格段新', '通用容量段新', '洗涤价格段改', '洗涤容量段改', '小天鹅价格段', '小天鹅容量段',
                       '大区', '美的渠道', '省份', '洗衣机美的片区', '洗衣机美的中心', '洗衣机美的中心2'
                       ]
周度线下干衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '干衣方式', '上市月度', '上市周度', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                      '线下价格段', '线下容量段']

# 正式环境
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/avc")


# 测试环境
# engine=create_engine("mysql+pymysql://test:test123@10.157.6.74:3306/ods")

def avc_month_xlsx2db(file_path, table_name, sheet_name, type_name, cols, header):
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=cols)
    df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)


def avc_week_xlsx2db(file_path, table_name, sheet_name, type_name, cols, header):
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=cols)
    df['年'] = df['周度'].map(lambda x: x.split('W')[0])
    df['周'] = df['周度'].map(lambda x: x.split('W')[1])
    df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)


def avc_month_csv2db(file_path, table_name, type_name, cols, header):
    df = pd.read_csv(file_path, header=header, usecols=cols, encoding="gbk")
    df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)


def avc_week_csv2db(file_path, table_name, type_name, cols, header):
    df = pd.read_csv(file_path, header=header, usecols=cols, encoding="gbk", engine='python')
    df['年'] = df['周度'].map(lambda x: x.split('W')[0])
    df['周'] = df['周度'].map(lambda x: x.split('W')[1])
    df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)


if __name__ == '__main__':
    # csvzhoudu2db(file_path=r'E:\Share\每日导数\19线下洗衣机周度永久表.txt',table_name='周度线下洗衣机',type_name='replace',cols=zhoudu_xianxiaxiyiji,header=0)
    # csvzhoudu2db(file_path=r'E:\Share\每日导数\20线下洗衣机周度永久表.txt',table_name='周度线下洗衣机',type_name='append',cols=zhoudu_xianxiaxiyiji,header=0)
    # avczhoudu2db(file_path=r'E:\Share\每日导数\19线下洗衣机周度.xlsx', table_name='周度线下洗衣机', sheet_name='线下洗衣机周度永久表', type_name='replace',
    #                cols=zhoudu_xianxiaxiyiji, header=0)
    # avczhoudu2db(file_path=r'E:\Share\每日导数\20线下洗衣机周度.xlsx', table_name='周度线下洗衣机', sheet_name='线下洗衣机周度永久表', type_name='append',
    #                cols=zhoudu_xianxiaxiyiji, header=0)
    # avczhoudu2db(file_path=r'E:\Share\每日导数\线上洗衣机20年周度.xlsx', table_name='周度线上洗衣机', sheet_name='源数据', type_name='append',
    #                cols=zhoudu_xianshangxiyiji, header=0)
    # excel2db(file_path=r'E:\Share\每日导数\品牌变化配置表.xlsx', table_name='品牌变化配置表', sheet_name='源数据', type_name='replace',
    #          cols=yuedu_xianxiaganyiji, header=0)
    # excel2db(file_path=r'E:\Share\每日导数\线下干衣机月度20.7-20.9.xlsx', table_name='月度线下干衣机', sheet_name='源数据', type_name='append',
    #                cols=yuedu_xianshangganyiji, header=0)
    # df = pd.read_excel(r'E:\Share\每日导数\品牌变化配置表.xlsx', sheet_name='源数据', header=0)
    # df.to_sql(name='品牌变化配置表', con=engine, if_exists='replace', index=False)
    # csv2db(file_path=r'E:\Share\每日导数\report.csv', table_name='线下干衣机月度永久表', type_name='replace',cols=None,header=0)
    avc_month_csv2db(file_path=r'E:\Share\每日导数\20年10月线下洗衣机月度永久表.txt', table_name='月度线下洗衣机', type_name='append',
                     cols=月度线下洗衣机, header=0)
