import pymysql
import pandas as pd
import cpca
from sqlalchemy import create_engine


conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.')

engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/dim')

sql = '''
    select
        distinct 
        主体名称,
        客户编码,
        客户名称,
        营销中心名称,
        划拨中心名称,
        收货地址
    from
        ods.发货数据明细
    where
        营销中心名称 = '苏锡常产品管理中心-网批';
    '''

df = pd.read_sql(sql, con=conn)

parsed_address = cpca.transform(df['收货地址'], cut=False)

location = pd.concat([df, parsed_address], axis=1)

location.to_sql('苏锡常网批客户地址', con=engine, if_exists='replace', index=False)

