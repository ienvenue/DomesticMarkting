import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pymysql
from datetime import datetime
import re
from openpyxl import load_workbook
import auto_send_email as mail


# 导入数据库

# 商品型号处理
def type_transform(ser_str):
    if type(ser_str) is float:
        return ser_str
    else:
        return re.sub('\（.*\）', '', ser_str.split(' ')[0]).replace('(2)', '')


# 分部中心处理
def center_name_transform(ser_str):
    return ser_str.replace('小天鹅', '').replace('美的', '').replace('产品管理中心', '')


# 产品线处理
def product_line_transform(ser_str):
    return ser_str.replace('小天鹅', 'LS').replace('美的', 'MD').replace('COLMO', 'MD').replace('比佛利', 'LS').replace('东芝',
                                                                                                                'DZ')

# 添加system列（新）
def add_system(ser):
    if ser in ['国美', '苏宁', '五星', 'TOP']:
        return ser


# 添加brand_1列
def add_brand_1(ser):
    if ser.find('LS') != -1:
        return 'ls'
    elif ser.find('MD') != -1:
        return 'md'
    elif ser.find('DZ') != -1:
        return 'dz'


# 添加brand_2列
def add_brand_2(goods_type, product_line):
    try:
        if 'BVL' in goods_type:
            return 'BVL'
        elif 'LS' in product_line and 'BVL' not in goods_type:
            return 'ls_no_BVL'
        elif 'MD' in product_line and goods_type not in (
        'CLDC10', 'CLGG15E', 'CLDG15T', 'CLDQ10', 'CLGQ10', 'CLDC12', 'CLHZ10'):
            return 'md'
        elif goods_type in ('CLDC10', 'CLGG15E', 'CLDG15T', 'CLDQ10', 'CLGQ10', 'CLDC12', 'CLHZ10'):
            return 'COLMO'
    except:
        pass


# 添加clean_type列
def add_clean_type(clean_type, goods_type):
    if clean_type is np.nan:
        try:
            if goods_type[1] == 'B' or goods_type[4] in ('B', 'J', 'F'):
                return '全自动'
            elif (goods_type[1] == 'D' and goods_type[:3] != 'VDL') or goods_type[1] == 'G' or goods_type[4] in ('D', 'G'):
                return '滚筒'
            elif goods_type[1] == 'H' or goods_type[4] == 'H':
                return '干衣机'
            elif goods_type[1] == 'P':
                return '双桶'
            else:
                return '其他'
        except:
            pass
    else:
        return clean_type


# 产品定位处理
def series_transform(level_des, series):
    if level_des is not np.nan:
        return level_des
    elif level_des is np.nan and series in ('初见', '超微净泡水魔方'):
        return '中高端'


# 添加add_centre_name_n列
def add_centre_name_n(ser):
    if ser == '海南':
        return '广州'
    elif ser == '内蒙古':
        return '北京'
    elif ser == '宜昌':
        return '武汉'
    elif ser == '芜湖':
        return '合肥'
    elif ser == '常德':
        return '长沙'
    else:
        return ser


# 导入配置表
conn = pymysql.connect(host='10.157.6.88', user='data_dev', password='data_dev0.', database='pank')
conf_product_level = pd.read_sql('select * from pank.conf_product_level', con=conn)
conf_product_series = pd.read_sql('select * from pank.conf_product_series', con=conn)

# 这里更改源文件路径
file_path = r'\\10.157.2.94\共享文件\固定报表\日报\零售导入.xlsx'
t1 = datetime.now()
print('开始读取Excel:{}'.format(t1))
df = pd.read_excel(file_path, sheet_name='Sheet0',
                   usecols=['商品型号', '商品编码', '大类', '小类', '门店编码', '门店名称', '门店等级', '门店一级分类', '门店二级分类', '数量', '单价', '总价',
                            '资源抵扣金额', '分部名称', '产品线', '创建时间'])
t2 = datetime.now()
print('读取完毕，耗时{}'.format(t2 - t1))
print('开始处理数据')
df['商品型号'] = df['商品型号'].map(type_transform)
df['分部名称'] = df['分部名称'].map(center_name_transform)
df['产品线'] = df['产品线'].map(product_line_transform)

df.rename(columns={'商品型号': 'goods_type', '商品编码': 'goods_imei', '大类': 'type_1', '小类': 'type_2', '门店编码': 'store_id',
                   '门店名称': 'store_name', '门店等级': 'store_rank', '门店一级分类': 'store_type_1', '门店二级分类': 'store_type_2',
                   '数量': 'sale_num', '单价': 'sale_price', '总价': 'total_price', '资源抵扣金额': 'deduction_price',
                   '分部名称': 'centre_name', '产品线': 'product_line', '创建时间': 'create_date', '时间戳': 'create_min'},
          inplace=True)

df['create_min'] = ''
df['system'] = df['store_type_1'].map(add_system)
df['brand_1'] = df['product_line'].map(add_brand_1)
df['brand_2'] = df.apply(lambda x: add_brand_2(x.goods_type, x.product_line), axis=1)
# 添加series和clean_type列
df = pd.merge(df, conf_product_series[['goods_imei', 'series', 'clean_type']], how='left', on='goods_imei')
df['series'] = df['series'].fillna('其他')
# 添加level_des列
df = pd.merge(df, conf_product_level[['goods_imei', 'level_des']], how='left', on='goods_imei')
df['clean_type'] = df.apply(lambda x: add_clean_type(x.clean_type, x.goods_type), axis=1)
df['level_des'] = df.apply(lambda x: series_transform(x.level_des, x.series), axis=1)
# 添加centre_name_n列
df['centre_name_n'] = df['centre_name'].map(add_centre_name_n)
t3 = datetime.now()
print('数据处理完毕，耗时{}'.format(t3 - t2))

# 存储到MYSQL
engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.6.88:3306/pank')
print('正在存储到数据库...')
# df.to_sql('m_sales_all', con=engine, if_exists='append', index=False)
t4 = datetime.now()
print('存储完毕，耗时{}'.format(t4 - t3))
print('存储总耗时{}'.format(t4 - t1))


# 导出数据
def write_excel(df, sheet_name):
    book = load_workbook(file)
    writer = pd.ExcelWriter(file, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet_name,index=False)
    writer.save()

t1 = datetime.now()
print('正在导出数据{}'.format(t1))

file = r'\\10.157.2.94\共享文件\固定报表\日报\零售日报.xlsx'

# 中心日报今年
center_this_year_sql = '''
select
	a.centre_name_n,
	小天鹅年度累计,
	小天鹅月年度累计,
	小天鹅不含比佛利年度累计,
	小天鹅不含比佛利月度累计,
	COLMO和比佛利年度累计,
	COLMO和比佛利月度累计,
	美的不含COLMO年度累计,
	美的不含COLMO月度累计,
	i.area ,
	美的系年度累计,
	美的系月度累计
from
	(
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'ls'
		and year(create_date) = year(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) a
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅月年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'ls'
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) b on
	a.centre_name_n = b.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅不含比佛利年度累计
	from
		m_sales_all mst
	where
		brand_2 = 'ls_no_BVL'
		and year(create_date) = year(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) c on
	a.centre_name_n = c.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅不含比佛利月度累计
	from
		m_sales_all mst
	where
		brand_2 = 'ls_no_BVL'
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) d on
	a.centre_name_n = d.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as COLMO和比佛利年度累计
	from
		m_sales_all mst
	where
		brand_2 in ('COLMO',
		'BVL')
		and year(create_date) = year(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) e on
	a.centre_name_n = e.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as COLMO和比佛利月度累计
	from
		m_sales_all mst
	where
		brand_2 in ('COLMO',
		'BVL')
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) f on
	a.centre_name_n = f.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的不含COLMO年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and year(create_date) = year(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) g on
	a.centre_name_n = g.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的不含COLMO月度累计
	from
		m_sales_all mst
	where
		brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n ) h on
	a.centre_name_n = h.centre_name_n
left join (
	select
		distinct center, area
	from
		area_center_zhihuanwang) i on
	a.centre_name_n = i.center
left join(
	select
		centre_name_n , sum(total_price)/ 10000 as 美的系年度累计
	from
		m_sales_all mst
	where
		year(create_date) = year(curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) j on
	a.centre_name_n = j.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的系月度累计
	from
		m_sales_all mst
	where
		year(create_date) = year(curdate())
		and month (create_date) = month (curdate())
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) k on
	a.centre_name_n = k.centre_name_n
	'''

# 中心日报去年
center_last_year_sql = '''
select
	a.centre_name_n,
	小天鹅年度累计,
	小天鹅月年度累计,
	小天鹅不含比佛利年度累计,
	小天鹅不含比佛利月度累计,
	COLMO和比佛利年度累计,
	COLMO和比佛利月度累计,
	美的不含COLMO年度累计,
	美的不含COLMO月度累计,
	i.area,
	美的系年度累计,
	美的系月度累计
from
	(
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'ls'
		and pank.去年年同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) a
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅月年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'ls'
		and pank.去年月同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) b on
	a.centre_name_n = b.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅不含比佛利年度累计
	from
		m_sales_all mst
	where
		brand_2 = 'ls_no_BVL'
		and pank.去年年同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) c on
	a.centre_name_n = c.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 小天鹅不含比佛利月度累计
	from
		m_sales_all mst
	where
		brand_2 = 'ls_no_BVL'
		and pank.去年月同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) d on
	a.centre_name_n = d.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as COLMO和比佛利年度累计
	from
		m_sales_all mst
	where
		brand_2 in ('COLMO',
		'BVL')
		and pank.去年年同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) e on
	a.centre_name_n = e.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as COLMO和比佛利月度累计
	from
		m_sales_all mst
	where
		brand_2 in ('COLMO',
		'BVL')
		and pank.去年月同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) f on
	a.centre_name_n = f.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的不含COLMO年度累计
	from
		m_sales_all mst
	where
		brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and pank.去年年同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) g on
	a.centre_name_n = g.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的不含COLMO月度累计
	from
		m_sales_all mst
	where
		brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and pank.去年月同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n ) h on
	a.centre_name_n = h.centre_name_n
left join (
	select
		distinct center, area
	from
		area_center_zhihuanwang) i on
	a.centre_name_n = i.center
left join(
	select
		centre_name_n , sum(total_price)/ 10000 as 美的系年度累计
	from
		m_sales_all mst
	where
		pank.去年年同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) j on
	a.centre_name_n = j.centre_name_n
left join (
	select
		centre_name_n , sum(total_price)/ 10000 as 美的系月度累计
	from
		m_sales_all mst
	where
		pank.去年月同期(create_date)
		and `system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
	group by
		centre_name_n) k on
	a.centre_name_n = k.centre_name_n
	'''

# 大区客户日报今年
client_this_year_sql = '''
select
	a.area,
	a.system,
	小天鹅不含比佛利年度累计销额,
	小天鹅不含比佛利年度累计销量,
	小天鹅不含比佛利月度累计销额,
	小天鹅不含比佛利月度累计销量,
	COLMO和比佛利年度累计销额,
	COLMO和比佛利年度累计销量,
	COLMO和比佛利月度累计销额,
	COLMO和比佛利月度累计销量,
	美的不含COLMO年度累计销额,
	美的不含COLMO年度累计销量,
	美的不含COLMO月度累计销额,
	美的不含COLMO月度累计销量
from
	(
	select
		b.area, a.system, sum(a.total_price)/10000 as 小天鹅不含比佛利年度累计销额, sum(sale_num) as 小天鹅不含比佛利年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 = 'ls_no_BVL'
		and year(create_date) = year(curdate())
	group by
		b.area , a.system) a
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 小天鹅不含比佛利月度累计销额, sum(sale_num) as 小天鹅不含比佛利月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 = 'ls_no_BVL'
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
	group by
		b.area , a.system) b on
	a.area = b.area
	and a.`system` = b.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as COLMO和比佛利年度累计销额, sum(sale_num) as COLMO和比佛利年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 in ('COLMO',
		'BVL')
		and year(create_date) = year(curdate())
	group by
		b.area , a.system) c on
	a.area = c.area
	and a.`system` = c.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as COLMO和比佛利月度累计销额, sum(sale_num) as COLMO和比佛利月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 in ('COLMO',
		'BVL')
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
	group by
		b.area , a.system) d on
	a.area = d.area
	and a.`system` = d.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 美的不含COLMO年度累计销额, sum(sale_num) as 美的不含COLMO年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and year(create_date) = year(curdate())
	group by
		b.area , a.system) e on
	a.area = e.area
	and a.`system` = e.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 美的不含COLMO月度累计销额, sum(sale_num) as 美的不含COLMO月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and year(create_date) = year(curdate())
		and month(create_date) = month(curdate())
	group by
		b.area , a.system) f on
	a.area = f.area
	and a.`system` = f.system
	'''

# 大区客户日报去年
client_last_year_sql = '''
select
	a.area,
	a.system,
	小天鹅不含比佛利年度累计销额,
	小天鹅不含比佛利年度累计销量,
	小天鹅不含比佛利月度累计销额,
	小天鹅不含比佛利月度累计销量,
	COLMO和比佛利年度累计销额,
	COLMO和比佛利年度累计销量,
	COLMO和比佛利月度累计销额,
	COLMO和比佛利月度累计销量,
	美的不含COLMO年度累计销额,
	美的不含COLMO年度累计销量,
	美的不含COLMO月度累计销额,
	美的不含COLMO月度累计销量
from
	(
	select
		b.area, a.system, sum(a.total_price)/10000 as 小天鹅不含比佛利年度累计销额, sum(sale_num) as 小天鹅不含比佛利年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 = 'ls_no_BVL'
		and pank.去年年同期(create_date)
	group by
		b.area , a.system) a
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 小天鹅不含比佛利月度累计销额, sum(sale_num) as 小天鹅不含比佛利月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 = 'ls_no_BVL'
		and pank.去年月同期(create_date)
	group by
		b.area , a.system) b on
	a.area = b.area
	and a.`system` = b.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as COLMO和比佛利年度累计销额, sum(sale_num) as COLMO和比佛利年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 in ('COLMO',
		'BVL')
		and pank.去年年同期(create_date)
	group by
		b.area , a.system) c on
	a.area = c.area
	and a.`system` = c.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as COLMO和比佛利月度累计销额, sum(sale_num) as COLMO和比佛利月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_2 in ('COLMO',
		'BVL')
		and pank.去年月同期(create_date)
	group by
		b.area , a.system) d on
	a.area = d.area
	and a.`system` = d.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 美的不含COLMO年度累计销额, sum(sale_num) as 美的不含COLMO年度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and pank.去年年同期(create_date)
	group by
		b.area , a.system) e on
	a.area = e.area
	and a.`system` = e.system
left join (
	select
		b.area, a.system, sum(a.total_price)/10000 as 美的不含COLMO月度累计销额, sum(sale_num) as 美的不含COLMO月度累计销量
	from
		m_sales_all a
	left join (select distinct area ,center from pank.area_center_zhihuanwang) b on
		a.centre_name_n = b.center
	where
		a.`system` in ('国美',
		'苏宁',
		'五星',
		'TOP')
		and brand_1 = 'md'
		and brand_2 <> 'COLMO'
		and pank.去年月同期(create_date)
	group by
		b.area , a.system) f on
	a.area = f.area
	and a.`system` = f.system
	'''

# 监控型号
moniter_type_sql = '''
select a.goods_type, 今年销量,今年销额,今年均价,本月销量,本月销额,本月均价,昨天销量,昨天销额,昨天均价
from
(select goods_type, sum(sale_num) as 今年销量, sum(total_price)/10000 as 今年销额, sum(total_price)/sum(sale_num) as 今年均价
from m_sales_all mst
where goods_type in ('BVL3D240T6','BVL3J110IY','BVL2J110IT','BVL2J90VG','BVL1B100VG','BVL2H100TY','BVL1D100G6','BVL1D120G6','BVL1G210EY3','BVL1D100TY4T','BVL2D120TG6','BVL2D100TY6','BVL1G100G6','BVL1G100W6','BVL1D120TG66','BVL1D100EG6','BVL1D100TG6','BVL1G100TG6','BVL1G80EG6','BVL1D80TG6','BVL1FD150ITY6','BVL1F150G6','BVL1FG150EY6','B1FDC150TG6','BVL1FDC150IT6','BVL1D100EY','BVL1D80EY','BVL2HD110EY2','BVL1D100TT','BVL1D100NTY','BVL1D100PTT','BVL1D100NET','BVL1G100NET','CLGG15E','CLHZ10','CLDC10','CLDG15T','CLDQ10','CLGQ10','CLDC12','TH100-H16G','TH90VL8G','TH100-H32Y','TH80-H002G','TBM90PMU06DT','TB30-88HUCLY','TB100-6388WADCLY','TB100-6388WACLY','TB100PM02T','TB90-6388WADCLY','TB90-6388WACLY','TB30-08A','TH100-H36WT','TH100NH06WY','TH100VTH35','TH90SH02WG','TH90-H02WY','TH70VZ21S','TD100-14266WMIADT','TD100-14266WMADT','TD100PM02T','TD100NM06Y','TG100-14266WMIADT','TG100-14266WMADT','TG100PM02T','TD100P52WDY','TG100NM06Y','TD100-1452WDY','TG100-1452WDY','TG30-80WMAD','TG30-80WMAD-T45W','TG30-80WMADY','TH60-Z020','TH35-Z008Y','TD100-1432IDY','TD100-1432DY','TG100-1432DY','TG100-4632DG','TBJ110-8188WUADCLT','TBJ110-8088WUADCLT','TBM100-8188WUADCLT','TBM90-8188WUADCLT','TBM100P8188UDCLT','TBM100-8188UDCLY','TBM90-8188UDCLY','TBM100-8088WUDCLT','TD120-1636WMUIADT','TD100-1636WMUIADT','TD100-1436MUADT','TD100P366WMUDY','TD100-14366WMUDT','TD80-1436WMUADT','TG100-1436MUADT','TG120-12366WMUDT','TG100-14366WMUDT','TD100N366WMUD','TH30-Z02','MH100-H05J','MH90-L05J','MH60-Z003','MH70VZ30','MD100-1463WIDY','MG100-1463WIDY','MG100-1463DY','MBS100PT2WADT','MBS100QT2WAY','MBS100T2WADY','MBS90PT2WADT','MBS90QT2WAY','MBS90T2WADY','MH100-H1WY','MH100-H1W','MH90-H03Y','MH90-L1W','MH70VZ10-1','MH30-Z01','MH70VZ10','MHP1','MH30-Z01-T13','MH100VTH707WY','MD100K1','MD100PD3QCT','MD100N07Y','MG100K1','MD100-1455WDY','MG100PD3QCT','MG100N07Y','MG100-1455WDY','MD100PT2WADQCY','MD100T2WADQCY','MG100T2WADQCY','MD100PT1WDQCT','MD80T2WADQCY','MD100T1WDQC','MG80T2WADQCY','MG100T1WDQC','MG100NT2WADQC','MG120T1WD3','MG80T1WD','MH30-Z03WG')
and year(create_date) = year(curdate())
group by goods_type) a
left join
(select goods_type, sum(sale_num) as 本月销量, sum(total_price)/10000 as 本月销额, sum(total_price)/sum(sale_num) as 本月均价
from m_sales_all mst
where goods_type in ('BVL3D240T6','BVL3J110IY','BVL2J110IT','BVL2J90VG','BVL1B100VG','BVL2H100TY','BVL1D100G6','BVL1D120G6','BVL1G210EY3','BVL1D100TY4T','BVL2D120TG6','BVL2D100TY6','BVL1G100G6','BVL1G100W6','BVL1D120TG66','BVL1D100EG6','BVL1D100TG6','BVL1G100TG6','BVL1G80EG6','BVL1D80TG6','BVL1FD150ITY6','BVL1F150G6','BVL1FG150EY6','B1FDC150TG6','BVL1FDC150IT6','BVL1D100EY','BVL1D80EY','BVL2HD110EY2','BVL1D100TT','BVL1D100NTY','BVL1D100PTT','BVL1D100NET','BVL1G100NET','CLGG15E','CLHZ10','CLDC10','CLDG15T','CLDQ10','CLGQ10','CLDC12','TH100-H16G','TH90VL8G','TH100-H32Y','TH80-H002G','TBM90PMU06DT','TB30-88HUCLY','TB100-6388WADCLY','TB100-6388WACLY','TB100PM02T','TB90-6388WADCLY','TB90-6388WACLY','TB30-08A','TH100-H36WT','TH100NH06WY','TH100VTH35','TH90SH02WG','TH90-H02WY','TH70VZ21S','TD100-14266WMIADT','TD100-14266WMADT','TD100PM02T','TD100NM06Y','TG100-14266WMIADT','TG100-14266WMADT','TG100PM02T','TD100P52WDY','TG100NM06Y','TD100-1452WDY','TG100-1452WDY','TG30-80WMAD','TG30-80WMAD-T45W','TG30-80WMADY','TH60-Z020','TH35-Z008Y','TD100-1432IDY','TD100-1432DY','TG100-1432DY','TG100-4632DG','TBJ110-8188WUADCLT','TBJ110-8088WUADCLT','TBM100-8188WUADCLT','TBM90-8188WUADCLT','TBM100P8188UDCLT','TBM100-8188UDCLY','TBM90-8188UDCLY','TBM100-8088WUDCLT','TD120-1636WMUIADT','TD100-1636WMUIADT','TD100-1436MUADT','TD100P366WMUDY','TD100-14366WMUDT','TD80-1436WMUADT','TG100-1436MUADT','TG120-12366WMUDT','TG100-14366WMUDT','TD100N366WMUD','TH30-Z02','MH100-H05J','MH90-L05J','MH60-Z003','MH70VZ30','MD100-1463WIDY','MG100-1463WIDY','MG100-1463DY','MBS100PT2WADT','MBS100QT2WAY','MBS100T2WADY','MBS90PT2WADT','MBS90QT2WAY','MBS90T2WADY','MH100-H1WY','MH100-H1W','MH90-H03Y','MH90-L1W','MH70VZ10-1','MH30-Z01','MH70VZ10','MHP1','MH30-Z01-T13','MH100VTH707WY','MD100K1','MD100PD3QCT','MD100N07Y','MG100K1','MD100-1455WDY','MG100PD3QCT','MG100N07Y','MG100-1455WDY','MD100PT2WADQCY','MD100T2WADQCY','MG100T2WADQCY','MD100PT1WDQCT','MD80T2WADQCY','MD100T1WDQC','MG80T2WADQCY','MG100T1WDQC','MG100NT2WADQC','MG120T1WD3','MG80T1WD','MH30-Z03WG')
and pank.当月月累(create_date)
group by goods_type) b
on a.goods_type = b.goods_type
left join
(select goods_type, sum(sale_num) as 昨天销量, sum(total_price)/10000 as 昨天销额, sum(total_price)/sum(sale_num) as 昨天均价
from m_sales_all mst
where goods_type in ('BVL3D240T6','BVL3J110IY','BVL2J110IT','BVL2J90VG','BVL1B100VG','BVL2H100TY','BVL1D100G6','BVL1D120G6','BVL1G210EY3','BVL1D100TY4T','BVL2D120TG6','BVL2D100TY6','BVL1G100G6','BVL1G100W6','BVL1D120TG66','BVL1D100EG6','BVL1D100TG6','BVL1G100TG6','BVL1G80EG6','BVL1D80TG6','BVL1FD150ITY6','BVL1F150G6','BVL1FG150EY6','B1FDC150TG6','BVL1FDC150IT6','BVL1D100EY','BVL1D80EY','BVL2HD110EY2','BVL1D100TT','BVL1D100NTY','BVL1D100PTT','BVL1D100NET','BVL1G100NET','CLGG15E','CLHZ10','CLDC10','CLDG15T','CLDQ10','CLGQ10','CLDC12','TH100-H16G','TH90VL8G','TH100-H32Y','TH80-H002G','TBM90PMU06DT','TB30-88HUCLY','TB100-6388WADCLY','TB100-6388WACLY','TB100PM02T','TB90-6388WADCLY','TB90-6388WACLY','TB30-08A','TH100-H36WT','TH100NH06WY','TH100VTH35','TH90SH02WG','TH90-H02WY','TH70VZ21S','TD100-14266WMIADT','TD100-14266WMADT','TD100PM02T','TD100NM06Y','TG100-14266WMIADT','TG100-14266WMADT','TG100PM02T','TD100P52WDY','TG100NM06Y','TD100-1452WDY','TG100-1452WDY','TG30-80WMAD','TG30-80WMAD-T45W','TG30-80WMADY','TH60-Z020','TH35-Z008Y','TD100-1432IDY','TD100-1432DY','TG100-1432DY','TG100-4632DG','TBJ110-8188WUADCLT','TBJ110-8088WUADCLT','TBM100-8188WUADCLT','TBM90-8188WUADCLT','TBM100P8188UDCLT','TBM100-8188UDCLY','TBM90-8188UDCLY','TBM100-8088WUDCLT','TD120-1636WMUIADT','TD100-1636WMUIADT','TD100-1436MUADT','TD100P366WMUDY','TD100-14366WMUDT','TD80-1436WMUADT','TG100-1436MUADT','TG120-12366WMUDT','TG100-14366WMUDT','TD100N366WMUD','TH30-Z02','MH100-H05J','MH90-L05J','MH60-Z003','MH70VZ30','MD100-1463WIDY','MG100-1463WIDY','MG100-1463DY','MBS100PT2WADT','MBS100QT2WAY','MBS100T2WADY','MBS90PT2WADT','MBS90QT2WAY','MBS90T2WADY','MH100-H1WY','MH100-H1W','MH90-H03Y','MH90-L1W','MH70VZ10-1','MH30-Z01','MH70VZ10','MHP1','MH30-Z01-T13','MH100VTH707WY','MD100K1','MD100PD3QCT','MD100N07Y','MG100K1','MD100-1455WDY','MG100PD3QCT','MG100N07Y','MG100-1455WDY','MD100PT2WADQCY','MD100T2WADQCY','MG100T2WADQCY','MD100PT1WDQCT','MD80T2WADQCY','MD100T1WDQC','MG80T2WADQCY','MG100T1WDQC','MG100NT2WADQC','MG120T1WD3','MG80T1WD','MH30-Z03WG')
and create_date = pank.yesterday_of_the_month()
group by goods_type) c
on a.goods_type = c.goods_type
union all
select a.brand_1, 今年销量,今年销额,今年均价,本月销量,本月销额,本月均价,昨天销量,昨天销额,昨天均价
from
(select brand_1, sum(sale_num) as 今年销量, sum(total_price)/10000 as 今年销额, sum(total_price)/sum(sale_num) as 今年均价
from m_sales_all mst
where year(create_date) = year(curdate())
group by brand_1) a
left join
(select brand_1, sum(sale_num) as 本月销量, sum(total_price)/10000 as 本月销额, sum(total_price)/sum(sale_num) as 本月均价
from m_sales_all mst
where pank.当月月累(create_date)
group by brand_1) b
on a.brand_1 = b.brand_1
left join
(select brand_1, sum(sale_num) as 昨天销量, sum(total_price)/10000 as 昨天销额, sum(total_price)/sum(sale_num) as 昨天均价
from m_sales_all mst
where create_date = pank.yesterday_of_the_month()
group by brand_1) c
on a.brand_1 = c.brand_1
'''

# 连接潘坤数据库
engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.6.88:3306/pank')

t1 = datetime.now()
# 读取中心日报今年数据
print('正在读取中心日报今年数据{}'.format(t1))
df1 = pd.read_sql(center_this_year_sql, con=engine)
t2 = datetime.now()
print('中心日报今年数据读取完毕{}'.format(t2-t1))

t3 = datetime.now()
# 读取中心日报去年数据
print('正在读取中心日报去年数据{}'.format(t3))
df2 = pd.read_sql(center_last_year_sql, con=engine)
print('中心日报去年数据读取完毕{}'.format(t3-t2))

t4 = datetime.now()
# 读取大区日报今年数据
print('正在读取大区日报今年数据{}'.format(t4))
df3 = pd.read_sql(client_this_year_sql, con=engine)
print('大区日报今年数据读取完毕{}'.format(t4-t3))

t5 = datetime.now()
# 读取大区日报去年数据
print('正在读取大区日报去年数据{}'.format(t5))
df4 = pd.read_sql(client_last_year_sql, con=engine)
print('大区日报去年数据读取完毕{}'.format(t5-t4))

t6 = datetime.now()
# 读取监控型号数据
print('正在读取监控型号数据{}'.format(t6))
df5 = pd.read_sql(moniter_type_sql, con=engine)
print('监控型号数据读取完毕{}'.format(t6-t5))


write_excel(df1, '中心日报源今年')
write_excel(df2, '中心日报源去年')
write_excel(df3, '大区日报今年')
write_excel(df4, '大区日报去年')
write_excel(df5, '监控型号')

t7 = datetime.now()
print('导出完成，共耗时:{}'.format(t7-t1))


t8 = datetime.now()
print('正在发送邮件')
recipients = ''' Panda 潘坤 <pankun1@midea.com>; ex_chenyj12@partner.midea.com;
 Monty Zhang 张梦迪 <zhangmd10@midea.com>; ex_tanlm1@partner.midea.com '''
cc_to = ''' Panda 潘坤 <pankun1@midea.com>; ex_chenyj12@partner.midea.com;
 Monty Zhang 张梦迪 <zhangmd10@midea.com>; ex_tanlm1@partner.midea.com '''

# recipients = '''
#   Yifeng Huang 黄一峰 <yifeng.huang@midea.com>; Kaifei Su 苏凯飞 <sukf@midea.com>; Mack 毛鑫 <maoxin@midea.com>;
#   Xiaodong Zhu 朱晓东 <zhuxd@midea.com>; Sawyer 孙昱寰 <sunyh3@midea.com>; Haifeng Liang 梁海峰 <lianghf5@midea.com>;
#   Rick Chen 陈可力 <keli.chen@midea.com>; Xin Liu 刘鑫 <liuxin17@midea.com>; Xiaohong Feng 冯小红 <fengxh3@midea.com>;
#   Jinlan Chen 陈锦兰 <chenjl3@midea.com>; Guangming Wang 王广明 <wanggm2@midea.com>; Yan Cao 曹岩 <caoyan1@midea.com>;
#   Peize Sun 孙佩泽 <peize.sun@midea.com>; felix 万方 <wanfang@midea.com>; Shuo Cui 崔硕 <shuo.cui@midea.com>;
#   Suzette 张雪钰 <zhangxy79@midea.com>; Wei Liang 梁炜 <wei1.liang@midea.com>; Yuntao Jia 郏云涛 <jiayt@midea.com>;
#   Dongdong Yang 杨冬冬 <dongdong.yang@midea.com>; Zhe Li 李哲 <zhe2.li@midea.com>; Jing Chen 陈静 <chj@midea.com>;
#   Feng Pan 潘峰 <panfeng@midea.com>; Zhichao Wang 王志超 <wangzc2@midea.com>; Karen 张可然 <zhangkr@midea.com>;
#   Binyang Wang 王斌阳 <binyang.wang@midea.com>; Hui Xu 徐惠 <hui3.xu@midea.com>; 堵维伟 <duww3@midea.com>;
#   Zheng Gong 宫正 <gongzheng@midea.com>; hongyu 洪宇 <hongyu@midea.com>; Hugo 于国新 <guoxin.yu@midea.com>;
#   Gene 吉九燃 <jijr1@midea.com>; Fuxing Ding 丁付行 <dingfx@midea.com>; DANGHUI CHEN 陈党辉 <chendh14@midea.com>;
#   Jenny 齐娟<qijuanxyj@midea.com>;Hongmei Wang 王红梅<hongmei2.wang@midea.com>;gu mingli 顾明丽<mingli.gu@midea.com>;
#   yuan na na 原娜娜<nana1.yuan@midea.com>;Li Chen 陈莉<chenli@midea.com>;Zhi Chen 陈志<zhi1.chen@midea.com>;
#   Tony Chan 陈涛<chentao@midea.com>;wubin.wang@midea.com;Panda 潘坤 <pankun1@midea.com>;
#   Monty Zhang 张梦迪 <zhangmd10@midea.com>; ex_chenyj12@partner.midea.com
# '''
# cc_to = 'Louis 赵磊<zhaolei2@midea.com>;Chunkai Wang 王春凯<wangck1@midea.com>'

# file = r'\\10.157.2.94\共享文件\固定报表\日报\零售日报.xlsx'
send_email = mail.SendEmail(recipients, cc_to, file)
send_email.send_outlook()
t9 = datetime.now()
print('邮件发送完毕，耗时{}'.format(t9-t8))
