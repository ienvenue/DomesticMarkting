import pandas as pd
from sqlalchemy import create_engine
from openpyxl import load_workbook
from datetime import datetime

engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods')

file = r'\\10.157.2.94\共享文件\Python\太空舱洗烘套.xlsx'

# 写入多个sheet页需要使用ExcelWriter
def write_excel(df, sheet_name):
    book = load_workbook(file)
    book.remove(book[sheet_name])
    writer = pd.ExcelWriter(file, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet_name, index=False)
    writer.save()

def total(ser1, ser2):
    if ser1 > 0 and ser2 > 0:
        return min(ser1, ser2)
    elif ser1 < 0 and ser2 < 0:
        return max(ser1, ser2)
    else:
        return 0

# 洗烘套装
wash_dry_sql = '''
select
	c.中心,
	b.品类,
	数量,
	顾客手机
from
	ods.mmp零售数据全量 a
inner join ods.太空舱洗烘套 b on
	a.商品型号 = b.型号
left join ods.中心分部配置 c on
	a.分部名称 = c.分部名称
where
	创建时间 between '2020-10-23' and now();
'''

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取太空船洗烘套装数据 {}'.format(t1))
df4 = pd.read_sql(wash_dry_sql, con=engine)
df4 = pd.pivot_table(df4, values='数量', index=['中心','顾客手机'], columns='品类', aggfunc='sum', fill_value=0)
df4['合计'] = df4.apply(lambda x: total(x.干衣机, x.滚筒), axis=1)
df4 = df4.reset_index()
df4 = df4.groupby(df4['中心']).合计.sum().reset_index()
write_excel(df4, '洗烘套装')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('太空船洗烘套装写入完成 {}'.format(t2))