import pandas as pd
from sqlalchemy import create_engine
import pymysql
import datetime
import re
import time

# 昨天
yesterday = datetime.date.today() + datetime.timedelta(days = -1)

engine = create_engine('mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods')

start_date = input('请输入开始日期：')
while re.match(r'^20\d{2}-[01]\d-[0123]\d$', start_date) is None:
    start_date = input('日期格式有误，请重新输入：')

end_date = input('请输入结束日期：')
while re.match(r'^20\d{2}-[01]\d-[0123]\d$', end_date) is None:
    end_date = input('日期格式有误，请重新输入：')

print('开始计算！')

# 洗烘套装合计
def total(ser1, ser2):
    if ser1 > 0 and ser2 > 0:
        return min(ser1, ser2)
    elif ser1 < 0 and ser2 < 0:
        return max(ser1, ser2)
    else:
        return 0

# 手工计算套装，日期从2021-02-01到2021-02-21
wash_dry_manual_sql = '''
select
    a.创建时间,
    a.门店编码,
	a.商品编码,
	数量,
	顾客手机
from
	ods.mmp零售数据全量 a
inner join ods.冰洗套装编码 b on
	a.商品编码 = b.单品编码 
where 创建时间 between '{0}' and '{1}'
union all 
select
    a.创建时间,
    a.门店编码,
	a.商品编码,
	数量,
	顾客手机
from
	ods.mmp冰箱 a
inner join ods.冰洗套装编码 b on
	a.商品编码 = b.单品编码 
where 创建时间 between '{0}' and '{1}'
    '''.format(start_date, end_date)

df = pd.read_sql(wash_dry_manual_sql, con=engine)

df = df.groupby(['创建时间','门店编码','顾客手机','商品编码']).sum().reset_index()

df = df.pivot_table('数量',['创建时间','门店编码','顾客手机'],'商品编码',aggfunc='sum',fill_value=0)

taozhuang_lst = [['21038120Z01521', '31031040002527'], ['21038120Z01522', '31031040002527'],
                 ['21038120Z00181', '31031040002527'], ['21038120Z00201', '31031040002527'],
                 ['21038120001209', '31031040003323'], ['21038120001209', '31031040003523'],
                 ['21038120001209', '31031040003324'], ['21038120001209', '31031040003364'],
                 ['21038120001209', '31031040003365'], ['21038120001209', '31031040003522'],
                 ['21038120001209', '31031040003362'], ['21038120001209', '31031040003363'],
                 ['21038120001209', '31031050001602'], ['21038120001589', '31031050002244'],
                 ['21038120001589', '31031040003682'], ['21038120001490', '31031040003324'],
                 ['21038120001490', '31031040003364'], ['21038120001490', '31031040003522'],
                 ['21038120001490', '31031040003323'], ['21038120001490', '31031040003362'],
                 ['21038120001490', '31031040003363'], ['21038120001490', '31031050001602'],
                 ['21038120001490', '31031040003523'], ['21038120001490', '31031040003365'],
                 ['21038110002413', '21038220000321'], ['21038110002873', '21038230000001'],
                 ['21038110002814', '21038220000481'], ['21038120001109', '31031040002122'],
                 ['21038120001109', '31031040002064'], ['21038120001109', '31031040001722'],
                 ['21038120001109', '31031040002842'], ['21038120001109', '31031040003664'],
                 ['21038120Z01083', '31031040002122'], ['21038120Z01083', '31031040001722'],
                 ['21038120Z01083', '31031040002064'], ['21038120Z01083', '31031040002842'],
                 ['21038120Z01083', '31031040003664'], ['21038120Z01121', '31031040002122'],
                 ['21038120Z01121', '31031040001722'], ['21038120Z01121', '31031040002064'],
                 ['21038120Z01121', '31031040002842'], ['21038120Z01121', '31031040003664'],
                 ['21038120Z00801', '31031040002122'], ['21038120Z00801', '31031040001722'],
                 ['21038120Z00801', '31031040002064'], ['21038120Z00801', '31031040002842'],
                 ['21038120Z00801', '31031040003664'], ['21038120Z00721', '31031040002122'],
                 ['21038120Z00721', '31031040001722'], ['21038120Z00721', '31031040002064'],
                 ['21038120Z00721', '31031040002842'], ['21038120Z00721', '31031040003664'],
                 ['21038120000889', '31031040002122'], ['21038120000889', '31031040001722'],
                 ['21038120000889', '31031040002064'], ['21038120000889', '31031040002842'],
                 ['21038120000889', '31031040003664'], ['21038120001449', '31031040002122'],
                 ['21038120001449', '31031040001722'], ['21038120001449', '31031040002064'],
                 ['21038120001449', '31031040002842'], ['21038120001449', '31031040003664'],
                 ['21038120000830', '31031040002122'], ['21038120000830', '31031040001722'],
                 ['21038120000830', '31031040002064'], ['21038120000830', '31031040002842'],
                 ['21038120000830', '31031040003664'], ['21038120000829', '31031040002122'],
                 ['21038120000829', '31031040001722'], ['21038120000829', '31031040002064'],
                 ['21038120000829', '31031040002842'], ['21038120000829', '31031040003664'],
                 ['21038110Z01541', '21038220000422'], ['21038110002433', '31031040002122'],
                 ['21038110002433', '31031040001722'], ['21038110002433', '31031040002064'],
                 ['21038110002433', '31031040002842'], ['21038110002433', '31031040003664'],
                 ['21038110Z01443', '31031040002122'], ['21038110Z01443', '31031040002064'],
                 ['21038110Z01443', '31031040001722'], ['21038110Z01443', '31031040002842'],
                 ['21038110Z01443', '31031040003664'], ['21038110Z01461', '31031040002122'],
                 ['21038110Z01461', '31031040001722'], ['21038110Z01461', '31031040002064'],
                 ['21038110Z01461', '31031040002842'], ['21038110Z01461', '31031040003664'],
                 ['21038110002213', '21038220000241'], ['21038110002773', '21038220000461'],
                 ['21038110002773', '31031040002122'], ['21038110002773', '31031040001722'],
                 ['21038110002773', '31031040002064'], ['21038110002773', '31031040002842'],
                 ['21038110002773', '31031040003664'], ['21038110002673', '21038220000261'],
                 ['21038110002133', '21038220000261'], ['21038110002133', '31031040002122'],
                 ['21038110002133', '31031040001722'], ['21038110002133', '31031040002064'],
                 ['21038110002133', '31031040002842'], ['21038110002133', '31031040003664'],
                 ['21038110002153', '31031040002122'], ['21038110002153', '31031040001722'],
                 ['21038110002153', '31031040002064'], ['21038110002153', '31031040002842'],
                 ['21038110002153', '31031040003664'], ['21038120001129', '31031040002122'],
                 ['21038120001129', '31031040002064'], ['21038120001129', '31031040002842'],
                 ['21038120001129', '31031040001722'], ['21038120001129', '31031040003664'],
                 ['21038120001130', '31031040002122'], ['21038120001130', '31031040002064'],
                 ['21038120001130', '31031040002842'], ['21038120001130', '31031040001722'],
                 ['21038120001130', '31031040003664'], ['21038120000871', '31031040002122'],
                 ['21038120000871', '31031040001722'], ['21038120000871', '31031040002064'],
                 ['21038120000871', '31031040002842'], ['21038120000871', '31031040003664'],
                 ['21038120Z00821', '31031040002122'], ['21038120Z00821', '31031040001722'],
                 ['21038120Z00821', '31031040002064'], ['21038120Z00821', '31031040002842'],
                 ['21038120Z00821', '31031040003664'], ['21038110002494', '21038220000301'],
                 ['21038110002494', '31031040002122'], ['21038110002494', '31031040002064'],
                 ['21038110002494', '31031040002842'], ['21038110002494', '31031040001722'],
                 ['21038110002494', '31031040003664'], ['21038110002493', '21038220000301'],
                 ['21038110002493', '31031040002122'], ['21038110002493', '31031040002064'],
                 ['21038110002493', '31031040002842'], ['21038110002493', '31031040001722'],
                 ['21038110002493', '31031040003664'], ['21038110002194', '21038220000301'],
                 ['21038110002194', '31031040002122'], ['21038110002194', '31031040001722'],
                 ['21038110002194', '31031040002064'], ['21038110002194', '31031040002842'],
                 ['21038110002194', '31031040003664'], ['21038110002075', '21038220000301'],
                 ['21038110002733', '21038220000441'], ['21038110Z01401', '21038220000341'],
                 ['21038110Z01481', '21038220000301'], ['21038110Z01201', '21038220000301'],
                 ['21038110002193', '31031040002122'], ['21038110002193', '31031040002064'],
                 ['21038110002193', '31031040002842'], ['21038110002193', '31031040001722'],
                 ['21038110002193', '31031040003664'], ['21038110002753', '21038220000521'],
                 ['71038110Z01082', '71038230Z00001'], ['21038120001350', '31031040003383'],
                 ['21038120001350', '31031040003382'], ['21038120001350', '31031040002922'],
                 ['21038120001350', '31031040002522'], ['21038120001350', '31031050002061']]


df_total = pd.DataFrame({'门店编码':[]})

for tz in taozhuang_lst:
    干衣机 = tz[1]
    滚筒 = tz[0]
    mark = 滚筒 + '+' + 干衣机
    try:
        frame = df[tz]
        frame[mark] = frame.apply(lambda x: total(x[干衣机], x[滚筒]), axis=1)
        frame = frame.reset_index()
        frame = frame.groupby(['门店编码'])[mark].sum().reset_index()
        df_total = pd.merge(df_total, frame, how='outer').fillna(0)
    except:
        df_total[mark] = 0

df_total = df_total.set_index('门店编码').stack().reset_index().rename(columns={'level_1':'套装',0:'套数'})
df2excel = df_total[df_total['套数'] != 0]
df2excel.to_excel(r'D:\冰洗套装销售{0}至{1}.xlsx'.format(start_date, end_date), index=False)

print('计算完成，请至D盘目录下查看！程序将在3秒后自动关闭！')
time.sleep(3)
