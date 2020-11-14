import pandas as pd
from sqlalchemy import create_engine
from openpyxl import load_workbook
from datetime import datetime

# mmp数据
mmp_sql = '''
   select
	b.中心,
	a.门店一级分类,
	sum(a.数量 * c.系数) as 销量,
	sum(a.总价 * c.系数)/10000 as 销额
from
	ods.mmp零售数据全量 a
inner join ods.干衣机型号配置表 c on 
	a.商品编码 = c.编号
left join dim.中心分部配置 b on
	a.分部名称 = b.分部名称
where
	a.门店一级分类 in ('苏宁',
	'国美',
	'五星',
	'TOP')
	and ods.当月月累(创建时间)
group by
	b.中心,
	a.门店一级分类;
	'''

# 一二级代理数据
channel_sql = '''
select
	a.中心,
	sum(a.销量) as 销量,
	sum(a.销额) as 销额
from
	((
	select
		b.中心 ,
		sum(a.开单数量 * c.系数) as 销量,
		sum(a.`开单金额(折前)` * c.系数)/ 10000 as 销额
	from
		ods.一级代理渠道零售数据 a
	inner join ods.干衣机型号配置表 c on 	
		a.商品编码 = c.编号
	left join dim.中心分部配置 b on
		a.中心名称 = b.分部名称
	where
		ods.当月月累(单据日期) 
		and `卖方合作模式大类(CRM)/一级分类(CMDM)` not in ('TOP', 'TOP客户')
	group by
		b.分部名称)
union all (
select
	b.中心 , sum(a.开单数量 * c.系数) as 销量, sum(a.`开单金额(折前)` * c.系数)/ 10000 as 销额
from
	ods.二级代理渠道零售数据 a
inner join ods.干衣机型号配置表 c on 
	a.商品编码 = c.编号
left join dim.中心分部配置 b on
	a.中心名称 = b.分部名称
where
	ods.当月月累(单据日期) 
	and `卖方合作模式大类(CRM)/一级分类(CMDM)` not in ('TOP', 'TOP客户')
group by
	b.分部名称)) a
group by
	a.中心;
	'''

# 堡垒客户数据
fortress_sql = '''
select
	a.中心,
	sum(a.销量) as 销量,
	sum(a.销额) as 销额
from
	((
	select
		b.中心 ,
		sum(a.开单数量 * d.系数) as 销量,
		sum(a.`开单金额(折前)` * d.系数)/10000 as 销额
	from
		ods.一级代理渠道零售数据 a
	inner join ods.堡垒客户配置表 c on
		a.卖方客户编码 = c.`客户编码（K编码）`
	inner join ods.干衣机型号配置表 d on
		a.商品编码 = d.编号
	left join dim.中心分部配置 b on
		a.中心名称 = b.分部名称
	where
		ods.当月月累(单据日期)
	group by
		b.分部名称)
union all (
select
	b.中心 , sum(a.开单数量 * d.系数) as 销量, sum(a.`开单金额(折前)` * d.系数)/10000 as 销额
from
	ods.二级代理渠道零售数据 a
left join dim.中心分部配置 b on
	a.中心名称 = b.分部名称
inner join ods.堡垒客户配置表 c on
	a.卖方客户编码 = c.`客户编码（K编码）`
inner join ods.干衣机型号配置表 d on
	a.商品编码 = d.编号
where
	ods.当月月累(单据日期)
group by
	b.分部名称)) a
group by
	a.中心;
'''

# 洗烘套装
wash_dry_sql = '''
select
	c.中心,
	b.品类,
	数量,
	顾客手机
from
	ods.mmp零售数据全量 a
inner join ods.洗烘套配置表 b on
	a.商品型号 = b.型号
left join ods.中心分部配置 c on
	a.分部名称 = c.分部名称
where
	ods.当月月累(创建时间);
'''

# 线下长尾
offline_tail_sql = '''
 select
	c.中心 ,
	sum(a.产品数量 * b.系数) as 销量 ,
	sum(a.结算金额 * b.系数) /10000 as 销额 
from
	ods.发货数据明细 a
inner join ods.干衣机型号配置表 b on
	substring_index(substring_index(产品编码, '"', 2), '"',-1) = b.编号
left join ods.中心分部配置 c on
	a.营销中心名称 = c.分部名称
where
	客户类型 = '线下长尾'
	and ods.当月月累(单据日期)
group by
	c.中心;
	'''

engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods')

file = r'\\10.157.2.94\共享文件\Python\干衣机模板.xlsx'

# 写入多个sheet页需要使用ExcelWriter
def write_excel(df, sheet_name):
    book = load_workbook(file)
    book.remove(book[sheet_name])
    writer = pd.ExcelWriter(file, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet_name, index=False)
    writer.save()

# 洗烘套装合计
def total(ser1, ser2):
    if ser1 > 0 and ser2 > 0:
        return min(ser1, ser2)
    elif ser1 < 0 and ser2 < 0:
        return max(ser1, ser2)
    else:
        return 0

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取零售数据 {}'.format(t1))
df1 = pd.read_sql(mmp_sql, con=engine)
# print('正在写入零售数据')
write_excel(df1, '零售')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('零售数据写入完成 {}'.format(t2))

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取代理数据 {}'.format(t1))
df2 = pd.read_sql(channel_sql, con=engine)
# print('正在写入代理数据')
write_excel(df2, '代理')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('代理数据写入完成 {}'.format(t2))

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取堡垒客户数据 {}'.format(t1))
df3 = pd.read_sql(fortress_sql, con=engine)
write_excel(df3, '堡垒')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('堡垒数据写入完成 {}'.format(t2))

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取洗烘套装数据 {}'.format(t1))
df4 = pd.read_sql(wash_dry_sql, con=engine)
df4 = pd.pivot_table(df4, values='数量', index=['中心','顾客手机'], columns='品类', aggfunc='sum', fill_value=0)
df4['合计'] = df4.apply(lambda x: total(x.干衣机, x.滚筒), axis=1)
df4 = df4.reset_index()
df4 = df4.groupby(df4['中心']).合计.sum().reset_index()
write_excel(df4, '洗烘套装')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('洗烘套装写入完成 {}'.format(t2))

t1 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('正在读取线下长尾数据 {}'.format(t1))
df5 = pd.read_sql(offline_tail_sql, con=engine)
write_excel(df5, '线下长尾')
t2 = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print('线下长尾写入完成 {}'.format(t2))