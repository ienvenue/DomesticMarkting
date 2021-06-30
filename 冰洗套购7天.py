import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import time
import pymysql


engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306')

# 冰洗套购合计
def total(ser1, ser2, ser3):
    if ser3 <= 7:
        if ser1 > 0 and ser2 > 0:
            return ser1 + ser2
        elif ser1 < 0 and ser2 < 0:
            return ser1 + ser2
        else:
            return 0
    else:
        return 0

xyj_sql = '''
select
   创建时间 as 洗衣机日期,
   门店编码,
   顾客手机,
   sum(总价) as 洗衣机销额
from
   ods.mmp零售数据全量 a
inner join ods.洗衣机产品基础表 b on
   a.商品编码 = b.产品编码
where
   创建时间 between '2021-05-06' and '2021-05-31'
   and 顾客手机 is not null
   and length(顾客手机) = 11
   and 门店一级分类 in ('苏宁',
   '国美',
   '五星',
   'TOP')
   and 门店二级分类 not in ('国美新零售',
   '苏宁零售云',
   '五星万镇通')
   and 导购员名称 is not null
group by
   创建时间,
   门店编码,
   顾客手机
order by 
   创建时间
    '''

bx_sql = '''
select
   创建时间 as 冰箱日期,
   门店编码,
   顾客手机,
   sum(总价) as 冰箱销额
from
   ods.mmp冰箱
where
   创建时间 between '2021-05-06' and '2021-05-31'
   and 商品编码 not like 'T%%'
   and 顾客手机 is not null
   and length(顾客手机) = 11
   and 门店一级分类 in ('苏宁',
   '国美',
   '五星',
   'TOP')
   and 门店二级分类 not in ('国美新零售',
   '苏宁零售云',
   '五星万镇通')
   and 导购员名称 is not null
group by
   创建时间,
   门店编码,
   顾客手机
order by
   创建时间
'''

df_xyj = pd.read_sql(xyj_sql, con=engine)
df_bx = pd.read_sql(bx_sql, con=engine)

df_merged = pd.merge(df_xyj, df_bx, on=['门店编码','顾客手机'])

df_merged['间隔'] = abs(df_merged['洗衣机日期'] - df_merged['冰箱日期'])
df_merged['间隔'] = df_merged['间隔'].map(lambda x: x/np.timedelta64(1, 'D'))

df_merged['套购金额'] = df_merged.apply(lambda x: total(x.洗衣机销额, x.冰箱销额, x.间隔), axis=1)

df_merged = df_merged[(abs(df_merged['洗衣机销额'])>=500) & (abs(df_merged['冰箱销额'])>=500)]
df_merged.to_excel(r'D:\冰洗套购7天间隔.xlsx', index=False)

print('已完成，系统将在30秒后退出')
time.sleep(30)

# df_merged_brand = pd.merge(df_xyj, df_bx, on=['门店编码','顾客手机','品牌'])

# df_merged_brand['间隔'] = abs(df_merged_brand['洗衣机日期'] - df_merged_brand['冰箱日期'])
# df_merged_brand['间隔'] = df_merged_brand['间隔'].map(lambda x: x/np.timedelta64(1, 'D'))

# df_merged_brand['套购金额'] = df_merged_brand.apply(lambda x: total(x.洗衣机销额, x.冰箱销额, x.间隔), axis=1)

# df_merged_brand = df_merged_brand[(abs(df_merged_brand['洗衣机销额'])>=500) & (abs(df_merged_brand['冰箱销额'])>=500)]
# df_merged_brand.to_excel(r'D:\MyData\zhang md10\Desktop\冰洗套购(品牌).xlsx', index=False)