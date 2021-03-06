import os
import time
import pandas as pd
import openpyxl
from sqlalchemy import create_engine
import zhihuanwan_log as zl

# 定义路径
file = r'\\10.157.2.94\临时文件\指环王数据每日制作11.24.xlsx'

# 定到门店sql
sql_mmp = '''select
    a.center_name as 中心,
    b.门店编码,
    case  when b.分部名称 like '美的%' then '美的' else '小天鹅' end as 品牌,
    b.积分 as 积分,
    b.台数 as 台数,
    b.高端结构机台数 as 高端台数
from
    ods.center_group_zhihuanwang a
    inner join (
        select
            c.center,
            a.门店编码,
            a.分部名称,
            sum(b.score * a.数量) as 积分,
            sum(a.数量) as 台数,
            sum(b.is_jiegouji * a.数量) as 高端结构机台数
        from
            ods.mmp零售数据全量 a
            inner join ods.model_score b on a.商品编码 = b.merch_id
            inner join ods.area_center_zhihuanwang c on c.center_name = a.分部名称
        where
        	a.创建时间 between '2020-08-28' and '2020-11-24' and 
            (
                (
                    a.门店一级分类 in ('苏宁', '国美', 'TOP', '五星', '商超')
                    and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
                ) #新增直营门店
                or (
                    a.门店编码 in (
                        'S00081607',
                        'S00081632',
                        'S00081635',
                        'S00081648',
                        'S00082093',
                        'S00091085',
                        'S00215530',
                        'S00089426',
                        'S00081003',
                        'S00081004',
                        'S00087008',
                        'S00191565',
                        'S00204520',
                        'S00081031',
                        'S00081155',
                        'S00090191',
                        'S00195186',
                        'S00081080',
                        'S00081541',
                        'S00090768',
                        'S00090769',
                        'S00090774',
                        'S00084355',
                        'S00084376',
                        'S00084392',
                        'S00084393',
                        'S00081818',
                        'S00084600',
                        'S00084648',
                        'S00078945',
                        'S00081418',
                        'S00081424',
                        'S00081437',
                        'S00068629',
                        'S00068639',
                        'S00013902',
                        'S00036059',
                        'S00036060',
                        'S00102010',
                        'S00081706',
                        'S00203272',
                        'S00081227',
                        'S00081228',
                        'S00083873',
                        'S00014418',
                        'S00084297',
                        'S00084755',
                        'S00088712',
                        'S00095896',
                        'S00081376',
                        'S00081383',
                        'S00081407',
                        'S00181472',
                        'S00239250',
                        'S00081394',
                        'S00081411',
                        'S00081701',
                        'S00082875',
                        'S00252523',
                        'S00081628',
                        'S00081695',
                        'S00081702',
                        'S00081714',
                        'S00081717',
                        'S00082886',
                        'S00253325',
                        'S00260399',
                        'S00023693',
                        'S00076764',
                        'S00095292',
                        'S00048922',
                        'S00270654',
                        'S00280139'
                    )
                )
            )
            and b.score > 0
        group by
            c.center,
            a.门店编码,
            a.分部名称
    ) b on a.center_name = b.center
where
    center <> '新疆'    
union
all
select
	a.center_name as 中心,
	    b.门店编码,
case  when b.分部名称 like '美的%' then '美的' else '小天鹅' end as 品牌,
    b.积分 as 积分,
    b.台数 as 台数,
    b.高端结构机台数 as 高端台数
from
    ods.center_group_zhihuanwang a
    inner join (
        select
            c.center,
            a.分部名称,
            a.门店编码,
            sum(b.score * a.数量) as 积分,
            sum(a.数量) as 台数,
            sum(b.is_jiegouji * a.数量) as 高端结构机台数
        from
            ods.mmp零售数据全量 a
            inner join ods.model_score b on a.商品编码 = b.merch_id
            inner join ods.area_center_zhihuanwang c on c.center_name = a.分部名称
        where
        	a.创建时间 between '2020-08-28' and '2020-11-24' and 
            a.门店一级分类 in ('苏宁', '国美', '五星', '商超')
            and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
            and b.score_xinjiang > 0
        group by
            c.center,
            a.门店编码
    ) b on a.center_name = b.center
where
    center = '新疆';
'''

# 定义渠道到门店sql
sql_channel = '''   
select
	d.center as 中心,
	d.卖方客户编码,
	d.门店编码,
	case  when d.品类 like '美的%' then '美的' else '小天鹅' end as 品牌,
	sum(d.score*d.`开单数量`) as 积分,
	sum(d.`开单数量`) as 台数,
	sum(d.is_jiegouji *d.`开单数量`) as 高端结构机台数
from
	(
	select
		c.center, a.卖方客户编码,a.门店编码,a.品类,b.score, a.开单数量,b.is_jiegouji 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where
		a.单据日期 between '2020-08-28' and '2020-11-24' and 
		a.`卖方合作模式大类(CRM)/一级分类(CMDM)` in ('TOP',
		'V200',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and b.score >0
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center,  a.卖方客户编码,a.门店编码,a.品类,b.score, a.开单数量,b.is_jiegouji 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
		and b.score >0
		and a.卖方客户名称 not like '已失效%' 
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
		where a.单据日期 between '2020-08-28' and '2020-11-24' ) d
where
	d.center <> '新疆'
group by
	d.center,
		d.卖方客户编码,
	d.品类
	,d.门店编码
union all
select
	c.center as 中心,
	'' as 卖方客户编码,
	 a.门店编码,
     case  when a.分部名称 like '美的%' then '美的' else '小天鹅' end as 品牌,
	sum(b.score_xinjiang * a.数量) as 积分,
	sum(a.数量) AS 台数,
	sum(b.is_jiegouji * a.数量) as 高端台数
from
	ods.mmp零售数据全量 a
inner join ods.model_score b on
	a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.创建时间 between '2020-08-28' and '2020-11-24' and 
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	and b.score_xinjiang >0
group by
	c.center,
	a.门店编码,
	a.分部名称
	'''

# 当月top&3c零售统计
sql_202010retail = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）,
sum(d.是否重点机型*a.数量) as 重点机型达成（台）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
inner join dim.零售门店分类 b
on b.门店一级分类 =a.门店一级分类 
and b.门店二级分类 =a.门店二级分类
where a.创建时间 between date_add(curdate(),interval -day(curdate())+1 day) and date_add(curdate(),interval -1 day)
and (b.类别 ='3C' or b.类别='TOP') 
group by c.中心
'''

# 去年同期top&3c零售统计
sql_mmp_last_month = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
inner join dim.零售门店分类 b
on b.门店一级分类 =a.门店一级分类 
and b.门店二级分类 =a.门店二级分类
where a.创建时间 between  date_add(date_add(curdate(),interval -1 month),interval -day(curdate())+1 day)  
and date_add(date_add(curdate(),interval -1 month),interval -1 day)
and (b.类别 ='3C' or b.类别='TOP') 
group by c.中心
'''

# 中心分系统top 3c零售数据
sql_202010top3c = ''' select c.中心,b.类别 , sum(a.总价)/10000 as 销额（万元）, 
            sum(a.数量) as 销量（台）,
            sum(a.总价)/sum(a.数量) as 销售均价（元）,
            sum(d.是否重点机型*a.数量) as 重点机型达成（台）
            from ods.mmp零售数据全量 a
            left join dim.连锁高端机配置表 d 
            on d.编码 = a.商品编码 
            inner join dim.中心分部配置 c on
            c.分部名称 = a.分部名称
            inner join dim.零售门店分类 b
            on b.门店一级分类 =a.门店一级分类 
            and b.门店二级分类 =a.门店二级分类
            where a.创建时间 between date_add(curdate(),interval -day(curdate())+1 day) and date_add(curdate(),interval -1 day)
            and (b.类别 ='3C' or b.类别='TOP') 
            group by c.中心,b.类别 '''

sql_202010date = '''
select c.中心,a.创建时间 , sum(a.总价)/10000 as 销额（万元）, 
            sum(a.数量) as 销量（台）,
            sum(a.总价)/sum(a.数量) as 销售均价（元）,
            sum(d.是否重点机型*a.数量) as 重点机型达成（台）
            from ods.mmp零售数据全量 a
            left join dim.连锁高端机配置表 d 
            on d.编码 = a.商品编码 
            inner join dim.中心分部配置 c on
            c.分部名称 = a.分部名称
            inner join dim.零售门店分类 b
            on b.门店一级分类 =a.门店一级分类 
            and b.门店二级分类 =a.门店二级分类
            where  datediff('2020-11-24', a.创建时间) <= 4
            and (b.类别 ='3C' or b.类别='TOP') 
            group by c.中心,a.创建时间 '''

sql_201910retail = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
            sum(a.数量) as 销量（台）,
            sum(a.总价)/sum(a.数量) as 销售均价（元）,
            sum(d.是否重点机型*a.数量) as 重点机型达成（台）
            from ods.mmp零售数据全量 a
            left join dim.连锁高端机配置表 d 
            on d.编码 = a.商品编码 
            inner join dim.中心分部配置 c on
            c.分部名称 = a.分部名称
            inner join dim.零售门店分类 b
            on b.门店一级分类 =a.门店一级分类 
            and b.门店二级分类 =a.门店二级分类
            where a.创建时间 between date_add(date_add(curdate(),interval -1 year),interval -day(curdate())+1 day) and date_add(date_add(curdate(),interval -1 year),interval -1 day)
            and (b.类别 ='3C' or b.类别='TOP') 
            group by c.中心
'''

# mmp零售
sql_mmp2 = '''
select a.center_group as 分组, a.center_name as 中心, a.target_score as 目标, b.score_1 as 积分, b.number1 as 台数, b.number2 as 高端台数
from ods.center_group_zhihuanwang a
inner join ( select c.center, sum(b.score * a.数量) as score_1, sum(a.数量) as number1, sum(b.is_jiegouji*a.数量) as number2
from ods.mmp零售数据全量 a
inner join ods.model_score b on
a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where ((a.门店一级分类 in ('苏宁', '国美', 'TOP', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通'))
-- 9.29新增v200与部分直营门店
or (a.门店编码 in ('S00081607', 'S00081632', 'S00081635', 'S00081648', 'S00082093', 'S00091085', 'S00215530', 'S00089426', 'S00081003', 'S00081004', 'S00087008', 'S00191565', 'S00204520', 'S00081031', 'S00081155', 'S00090191', 'S00195186', 'S00081080', 'S00081541', 'S00090768', 'S00090769', 'S00090774', 'S00084355', 'S00084376', 'S00084392', 'S00084393', 'S00081818', 'S00084600', 'S00084648', 'S00078945', 'S00081418', 'S00081424', 'S00081437', 'S00068629', 'S00068639', 'S00013902', 'S00036059', 'S00036060', 'S00102010', 'S00081706', 'S00203272', 'S00081227', 'S00081228', 'S00083873', 'S00014418', 'S00084297', 'S00084755', 'S00088712', 'S00095896', 'S00081376', 'S00081383', 'S00081407', 'S00181472', 'S00239250', 'S00081394', 'S00081411', 'S00081701', 'S00082875', 'S00252523', 'S00081628', 'S00081695', 'S00081702', 'S00081714', 'S00081717', 'S00082886', 'S00253325', 'S00260399', 'S00023693', 'S00076764', 'S00095292', 'S00048922', 'S00270654', 'S00280139')))
and b.score>0
and a.创建时间 between '2020-08-28' and '2020-11-24' 
group by c.center) b on
a.center_name = b.center
where center <> '新疆'
union all select a.center_group as 分组, a.center_name as 中心, a.target_score as 目标, b.score_1 as 积分, b.number1 as 台数, b.number2 as 高端台数
from ods.center_group_zhihuanwang a
inner join ( select c.center, sum(b.score_xinjiang * a.数量) as score_1, sum(a.数量) as number1, sum(b.is_jiegouji*a.数量) as number2
from ods.mmp零售数据全量 a
inner join ods.model_score b on
a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where a.门店一级分类 in ('苏宁', '国美', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
and b.score_xinjiang >0
and a.创建时间 between '2020-08-28' and '2020-11-24'
group by c.center) b on
a.center_name = b.center
where center = '新疆';
    '''

# 19年mmp零售
sql_19mmp_retail = '''
select a.center_group as 分组, a.center_name as 中心, a.target_score as 目标, b.score_1 as 积分, b.number1 as 台数, b.number2 as 高端台数
from ods.center_group_zhihuanwang a
inner join ( select c.center, sum(b.score * a.数量) as score_1, sum(a.数量) as number1, sum(b.is_jiegouji*a.数量) as number2
from ods.mmp零售数据全量 a
inner join ods.model_score b on
a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where ((a.门店一级分类 in ('苏宁', '国美', 'TOP', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通'))
-- 9.29新增v200与部分直营门店
or (a.门店编码 in ('S00081607', 'S00081632', 'S00081635', 'S00081648', 'S00082093', 'S00091085', 'S00215530', 'S00089426', 'S00081003', 'S00081004', 'S00087008', 'S00191565', 'S00204520', 'S00081031', 'S00081155', 'S00090191', 'S00195186', 'S00081080', 'S00081541', 'S00090768', 'S00090769', 'S00090774', 'S00084355', 'S00084376', 'S00084392', 'S00084393', 'S00081818', 'S00084600', 'S00084648', 'S00078945', 'S00081418', 'S00081424', 'S00081437', 'S00068629', 'S00068639', 'S00013902', 'S00036059', 'S00036060', 'S00102010', 'S00081706', 'S00203272', 'S00081227', 'S00081228', 'S00083873', 'S00014418', 'S00084297', 'S00084755', 'S00088712', 'S00095896', 'S00081376', 'S00081383', 'S00081407', 'S00181472', 'S00239250', 'S00081394', 'S00081411', 'S00081701', 'S00082875', 'S00252523', 'S00081628', 'S00081695', 'S00081702', 'S00081714', 'S00081717', 'S00082886', 'S00253325', 'S00260399', 'S00023693', 'S00076764', 'S00095292', 'S00048922', 'S00270654', 'S00280139')))
and b.score>0
and a.创建时间 between '2019-08-28' and date_add(date_add(curdate(),interval -1 year),interval -1 day)
group by c.center) b on
a.center_name = b.center
where center <> '新疆'
union all select a.center_group as 分组, a.center_name as 中心, a.target_score as 目标, b.score_1 as 积分, b.number1 as 台数, b.number2 as 高端台数
from ods.center_group_zhihuanwang a
inner join ( select c.center, sum(b.score_xinjiang * a.数量) as score_1, sum(a.数量) as number1, sum(b.is_jiegouji*a.数量) as number2
from ods.mmp零售数据全量 a
inner join ods.model_score b on
a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where a.门店一级分类 in ('苏宁', '国美', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
and b.score_xinjiang >0
and a.创建时间 between '2019-08-28' and date_add(date_add(curdate(),interval -1 year),interval -1 day)
group by c.center) b on
a.center_name = b.center
where center = '新疆';
    '''

# 渠道业务
sql_mmp3 = '''select
    a.门店一级分类,
    sum(b.score * a.数量) as 积分,
    sum(a.数量) as 台数,
    sum(b.is_jiegouji * a.数量) as 高端结构机台数
from
    ods.mmp零售数据全量 a
    inner join ods.model_score b on a.商品编码 = b.merch_id
    inner join ods.area_center_zhihuanwang c on c.center_name = a.分部名称
where
    (
        (
            a.门店一级分类 in ('苏宁', '国美', 'TOP', '五星', '商超')
            and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
        ) #新增直营门店
        or (
            a.门店编码 in (
                'S00081607',
                'S00081632',
                'S00081635',
                'S00081648',
                'S00082093',
                'S00091085',
                'S00215530',
                'S00089426',
                'S00081003',
                'S00081004',
                'S00087008',
                'S00191565',
                'S00204520',
                'S00081031',
                'S00081155',
                'S00090191',
                'S00195186',
                'S00081080',
                'S00081541',
                'S00090768',
                'S00090769',
                'S00090774',
                'S00084355',
                'S00084376',
                'S00084392',
                'S00084393',
                'S00081818',
                'S00084600',
                'S00084648',
                'S00078945',
                'S00081418',
                'S00081424',
                'S00081437',
                'S00068629',
                'S00068639',
                'S00013902',
                'S00036059',
                'S00036060',
                'S00102010',
                'S00081706',
                'S00203272',
                'S00081227',
                'S00081228',
                'S00083873',
                'S00014418',
                'S00084297',
                'S00084755',
                'S00088712',
                'S00095896',
                'S00081376',
                'S00081383',
                'S00081407',
                'S00181472',
                'S00239250',
                'S00081394',
                'S00081411',
                'S00081701',
                'S00082875',
                'S00252523',
                'S00081628',
                'S00081695',
                'S00081702',
                'S00081714',
                'S00081717',
                'S00082886',
                'S00253325',
                'S00260399',
                'S00023693',
                'S00076764',
                'S00095292',
                'S00048922',
                'S00270654',
                'S00280139'
            )
        )
    )
    and b.score > 0
    and a.创建时间 between '2020-08-28' and '2020-11-24'
group by
    a.门店一级分类; '''

# 代理渠道
sql_mmp4 = '''
select
	d.center as 中心,
	sum(d.score*d.`开单数量`) as 积分,
	sum(d.`开单数量`) 台数,
	sum(d.is_jiegouji *d.`开单数量`) as 高端台数
from
	(
	select
		c.center, b.score, a.开单数量,b.is_jiegouji 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where
	  a.单据日期 between '2020-08-28' and '2020-11-24'   and
		a.`卖方合作模式大类(CRM)/一级分类(CMDM)` in ('TOP',
		'V200',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and b.score >0
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center, b.score, a.开单数量,b.is_jiegouji 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
		and b.score >0
		and a.卖方客户名称 not like '已失效%' -- 新增剔除已失效客户
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
		where  a.单据日期 between '2020-08-28' and '2020-11-24'  ) d
where
	d.center <> '新疆'
group by
	d.center
union all
select
	c.center as 中心,
	sum(b.score_xinjiang * a.数量) as 积分,
	sum(a.数量) AS 台数,
	sum(b.is_jiegouji * a.数量) as 高端台数
from
	ods.mmp零售数据全量 a
inner join ods.model_score b on
	a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	and b.score_xinjiang >0
	and a.创建时间 between '2020-08-28' and '2020-11-24'
group by
	c.center
	'''

# 19年代理渠道
sql_19channel_retail = '''
select
	d.center as 中心,
	sum(d.score*d.`开单数量`) as 积分,
	sum(d.`开单数量`) 台数,
	sum(d.is_jiegouji *d.`开单数量`) as 高端台数
from
	(
	select
		c.center, b.score, a.开单数量,b.is_jiegouji 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where
	  a.单据日期 between '2019-08-28' and date_add(date_add(curdate(),interval -1 year),interval -1 day)  and
		a.`卖方合作模式大类(CRM)/一级分类(CMDM)` in ('TOP',
		'V200',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and b.score >0
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center, b.score, a.开单数量,b.is_jiegouji 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.model_score b on
		a.商品编码 = b.merch_id
		and b.score >0
		and a.卖方客户名称 not like '已失效%' -- 新增剔除已失效客户
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
		where   a.单据日期 between '2019-08-28' and date_add(date_add(curdate(),interval -1 year),interval -1 day) ) d
where
	d.center <> '新疆'
group by
	d.center
union all
select
	c.center as 中心,
	sum(b.score_xinjiang * a.数量) as 积分,
	sum(a.数量) AS 台数,
	sum(b.is_jiegouji * a.数量) as 高端台数
from
	ods.mmp零售数据全量 a
inner join ods.model_score b on
	a.商品编码 = b.merch_id
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	and b.score_xinjiang >0
    and a.创建时间 between '2019-08-28' and date_add(date_add(curdate(),interval -1 year),interval -1 day)
group by
	c.center
	'''

# 今年代理渠道销售台数份额
sql_channel_1 = '''
select
	d.center as 中心,
	sum(d.`开单数量`) as 今年台数,
	sum(d.常规促销价*d.开单数量)/10000 as 今年销售额（万元）,
	sum(d.常规促销价*d.开单数量)/sum(d.开单数量) as 今年销售均价（元）
from
	(
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号 =f.产品型号
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where (a.单据日期 between   date_add(curdate(),interval -day(curdate())+1 day)  and date_add(curdate(),interval -1 day))
	 and
		a.`卖方合作模式大类(crm)/一级分类(cmdm)` in ('top',
		'v100',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号=f.产品型号
		and a.卖方客户名称 not like '已失效%' -- 新增剔除已失效客户
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
			where  (a.单据日期 between   date_add(curdate(),interval -day(curdate())+1 day)  and date_add(curdate(),interval -1 day))) d
where
	d.center <> '新疆'
group by
	d.center
union all
select
	c.center as 中心,
	sum(a.数量) as 台数,
	sum(a.总价)/10000 as 销售额（万元）,
	sum(a.总价)/sum(a.数量) as 销售均价（元）
from
	ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	 and (a.创建时间 between   date_add(curdate(),interval -day(curdate())+1 day)  and date_add(curdate(),interval -1 day))
group by
	c.center
'''

# 去年代理渠道销售台数份额均价
sql_channel_2 = '''
select
	d.center as 中心,
	sum(d.`开单数量`) as  去年台数,
	sum(d.常规促销价*d.开单数量)/10000 as  去年销售额（万元）,
	sum(d.常规促销价*d.开单数量)/sum(d.开单数量) as  去年销售均价（元）
from
	(
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号 =f.产品型号
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where (a.单据日期 between   date_add(date_add(curdate(),interval -1 year),interval -day(curdate())+1 day)  and date_add(date_add(curdate(),interval -1 year),interval -1 day))
	 and
		a.`卖方合作模式大类(crm)/一级分类(cmdm)` in ('top',
		'v200',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号=f.产品型号
		and a.卖方客户名称 not like '已失效%' -- 新增剔除已失效客户
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
			where  (a.单据日期 between   date_add(date_add(curdate(),interval -1 year),interval -day(date_add(curdate(),interval -1 year))+1 day)  and date_add(date_add(curdate(),interval -1 year),interval -1 day))) d
where
	d.center <> '新疆'
group by
	d.center
union all
select
	c.center as 中心,
	sum(a.数量) as 台数,
	sum(a.总价)/10000 as 销售额（万元）,
	sum(a.总价)/sum(a.数量) as 销售均价（元）
from
	ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	 and (a.创建时间 between   date_add(date_add(curdate(),interval -1 year),interval -day(curdate())+1 day)  and date_add(date_add(curdate(),interval -1 year),interval -1 day))
group by
	c.center
'''

# 上个月代理渠道销售台数份额均价
sql_channel_3 = '''
select
	d.center as 中心,
	sum(d.`开单数量`) as 上个月台数,
	sum(d.常规促销价*d.开单数量)/10000 as 上个月销售额（万元）,
	sum(d.常规促销价*d.开单数量)/sum(d.开单数量) as 上个月销售均价（元）
from
	(
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.二级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号 =f.产品型号
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
	where (a.单据日期 between   date_add(date_add(curdate(),interval -1 month),interval -day(curdate())+1 day)  and date_add(date_add(curdate(),interval -1 month),interval -1 day))
	 and
		a.`卖方合作模式大类(crm)/一级分类(cmdm)` in ('top',
		'v100',
		'代理商',
		'多品店',
		'家装店',
		'零售代理',
		'零售商',
		'其他渠道',
		'旗舰店',
		'专卖店',
		'专业工程',
		'专业工程代理')
		and a.卖方客户名称 not like '已失效%'
union all
	select
		c.center, a.开单数量,f.常规促销价 
	from
		ods.一级代理渠道零售数据 a
	inner join ods.代理渠道常促价配置表 f
	on a.销售型号=f.产品型号
		and a.卖方客户名称 not like '已失效%' -- 新增剔除已失效客户
	inner join ods.area_center_zhihuanwang c on
		c.center_name = a.中心名称
			where  (a.单据日期 between   date_add(date_add(curdate(),interval -1 month),interval -day(curdate())+1 day)  and date_add(date_add(curdate(),interval -1 month),interval -1 day))) d
where
	d.center <> '新疆'
group by
	d.center
union all
select
	c.center as 中心,
	sum(a.数量) as 台数,
	sum(a.总价)/10000 as 销售额（万元）,
	sum(a.总价)/sum(a.数量) as 销售均价（元）
from
	ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
	c.center_name = a.分部名称
where
	a.门店一级分类 not in ( '苏宁',
	'国美')
	and c.center = '新疆'
	 and (a.创建时间 between   date_add(date_add(curdate(),interval -1 month),interval -day(curdate())+1 day)  and date_add(date_add(curdate(),interval -1 month),interval -1 day))
group by
	c.center
'''

# colmo销售情况
sql_colmo = '''
select d.center as 中心,d.单据日期 as 日期,d.商品编码,'代理' as 渠道 , sum(d.`开单数量`) as 销售台数, sum(d.常规促销价*d.开单数量)/ 10000 as 销售额（万元）, sum(d.常规促销价*d.开单数量)/ sum(d.开单数量) as 销售均价（元）
from ( select c.center, a.单据日期,a.商品编码,a.开单数量, f.常规促销价
from ods.二级代理渠道零售数据 a
inner join ods.代理渠道常促价配置表 f on
a.销售型号 = f.产品型号
inner join ods.area_center_zhihuanwang c on
c.center_name = a.中心名称
where a.商品编码 in ('21038120000769', '21038120Z00761', '21038120Z00741', '21038110002413', '21038220000321', '21038120001209', '21038110002573')
and (ods.当月月累(a.单据日期))
and a.`卖方合作模式大类(crm)/一级分类(cmdm)` in ('top', 'v100', '代理商', '多品店', '家装店', '零售代理', '零售商', '其他渠道', '旗舰店', '专卖店', '专业工程', '专业工程代理')
and a.卖方客户名称 not like '已失效%'
union all select c.center,  a.单据日期,a.商品编码,a.开单数量, f.常规促销价
from ods.一级代理渠道零售数据 a
inner join ods.代理渠道常促价配置表 f on
a.销售型号 = f.产品型号
and a.卖方客户名称 not like '已失效%'
-- 新增剔除已失效客户
inner join ods.area_center_zhihuanwang c on
c.center_name = a.中心名称
where (a.商品编码 in ('21038120000769', '21038120Z00761', '21038120Z00741', '21038110002413', '21038220000321', '21038120001209', '21038110002573')
and ods.当月月累(a.单据日期))) d
where d.center <> '新疆'
group by d.center,d.单据日期,d.商品编码
union all select c.center as 中心,a.创建时间 ,a.商品编码, '代理' as 渠道 ,sum(a.数量) as 销售台数, sum(a.总价)/ 10000 as 销售额（万元）, sum(a.总价)/ sum(a.数量) as 销售均价（元）
from ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where a.商品编码 in ('21038120000769', '21038120Z00761', '21038120Z00741', '21038110002413', '21038220000321', '21038120001209', '21038110002573')
and a.门店一级分类 not in ( '苏宁', '国美')
and c.center = '新疆'
and (ods.当月月累(a.创建时间))
group by c.center,a.创建时间 ,a.商品编码
union all 
select b.center as 中心,b.创建时间 as 日期,b.商品编码,'零售' as 渠道,b.销量（台）,b.销额（万元）,b.销售均价（元）
from ods.center_group_zhihuanwang a
inner join ( select c.center ,a.创建时间,a.商品编码, sum(a.总价)/10000 as 销额（万元）, sum(a.数量) as 销量（台）,sum(a.总价)/sum(a.数量) as 销售均价（元）
from ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where ((a.门店一级分类 in ('苏宁', '国美', 'TOP', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通'))
-- 9.29新增v200与部分直营门店
or (a.门店编码 in ('S00081607', 'S00081632', 'S00081635', 'S00081648', 'S00082093', 'S00091085', 'S00215530', 'S00089426', 'S00081003', 'S00081004', 'S00087008', 'S00191565', 'S00204520', 'S00081031', 'S00081155', 'S00090191', 'S00195186', 'S00081080', 'S00081541', 'S00090768', 'S00090769', 'S00090774', 'S00084355', 'S00084376', 'S00084392', 'S00084393', 'S00081818', 'S00084600', 'S00084648', 'S00078945', 'S00081418', 'S00081424', 'S00081437', 'S00068629', 'S00068639', 'S00013902', 'S00036059', 'S00036060', 'S00102010', 'S00081706', 'S00203272', 'S00081227', 'S00081228', 'S00083873', 'S00014418', 'S00084297', 'S00084755', 'S00088712', 'S00095896', 'S00081376', 'S00081383', 'S00081407', 'S00181472', 'S00239250', 'S00081394', 'S00081411', 'S00081701', 'S00082875', 'S00252523', 'S00081628', 'S00081695', 'S00081702', 'S00081714', 'S00081717', 'S00082886', 'S00253325', 'S00260399', 'S00023693', 'S00076764', 'S00095292', 'S00048922', 'S00270654', 'S00280139')))
and ods.当月月累(a.创建时间)
and a.商品编码 in ('21038120000769', '21038120Z00761', '21038120Z00741', '21038110002413', '21038220000321', '21038120001209', '21038110002573')
group by c.center,a.创建时间) b on
a.center_name = b.center
where center <> '新疆'
union all select b.center as 中心,b.创建时间 as 日期,b.商品编码,'零售' as 渠道,b.销量（台）,b.销额（万元）,b.销售均价（元）
from ods.center_group_zhihuanwang a
inner join (select c.center  ,a.创建时间,a.商品编码, sum(a.总价)/10000 as 销额（万元）, sum(a.数量) as 销量（台）,sum(a.总价)/sum(a.数量) as 销售均价（元）
from ods.mmp零售数据全量 a
inner join ods.area_center_zhihuanwang c on
c.center_name = a.分部名称
where a.门店一级分类 in ('苏宁', '国美', '五星', '商超')
and a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通')
and (ods.当月月累(a.创建时间))
and a.商品编码 in ('21038120000769', '21038120Z00761', '21038120Z00741', '21038110002413', '21038220000321', '21038120001209', '21038110002573')
group by c.center,a.创建时间) b on
a.center_name = b.center
where center = '新疆'
'''

# 去年零售同期
sql_quniantongqilingshou = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）,
sum(d.是否重点机型*a.数量) as 重点机型达成（台）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
inner join dim.零售门店分类 b
on b.门店一级分类 =a.门店一级分类 
and b.门店二级分类 =a.门店二级分类
where  ods.去年月同期(a.创建时间)
and (b.类别 ='3C' or b.类别='TOP') 
group by c.中心; '''

# 今年零售
sql_jinnianlingshou = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）,
sum(d.是否重点机型*a.数量) as 重点机型达成（台）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
inner join dim.零售门店分类 b
on b.门店一级分类 =a.门店一级分类 
and b.门店二级分类 =a.门店二级分类
where  ods.当月月累(a.创建时间)
and (b.类别 ='3C' or b.类别='TOP') 
group by c.中心; '''

# colmo专项
sql_colmozhuangxiang = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）,
sum(d.是否重点机型*a.数量) as 重点机型达成（台）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
where ods.当月月累(a.创建时间) and d.备注 ='COLMO'
group by c.中心;
'''
# 太空舱专项
sql_taikongcangzhuangxiang = '''
select c.中心, sum(a.总价)/10000 as 销额（万元）, 
sum(a.数量) as 销量（台）,
sum(a.总价)/sum(a.数量) as 销售均价（元）,
sum(d.是否重点机型*a.数量) as 重点机型达成（台）
from ods.mmp零售数据全量 a
left join dim.连锁高端机配置表 d 
on d.编码 = a.商品编码 
inner join dim.中心分部配置 c on
c.分部名称 = a.分部名称
where ods.当月月累(a.创建时间) and d.型号 in ('CLDG15T','CLGG15E')
group by c.中心;
'''

# 连接正式数据库
# 写入多个sheet页需要使用ExcelWriter
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")
book = openpyxl.load_workbook(file)
writer = pd.ExcelWriter(file, engine='openpyxl')
writer.book = book

def write_excel(df, sheet_name):
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet_name, index=False)



def export_data():
    zl.logger.warning("零售日报和指环王模型计算中，开始计算时间 :"+time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # 如果文件不存在，则创建
    if not os.path.exists(file):
        os.system(r"touch {}".format(file))

    sheet1=book["mmp到人员"]
    book.remove(sheet1)
    sheet2 = book["渠道到人员"]
    book.remove(sheet2)
    # 读取并写入数据
    df1=pd.read_sql(sql=sql_mmp, con=engine)
    write_excel(df=df1,sheet_name='mmp到人员')
    zl.logger.warning("指环王-mmp到门店，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df2=pd.read_sql(sql=sql_channel, con=engine)
    write_excel(df=df2,sheet_name='渠道到人员')
    zl.logger.warning("指环王-渠道到门店，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df3 = pd.read_sql(sql=sql_202010retail, con=engine)
    write_excel(df=df3, sheet_name='今年')
    zl.logger.warning("零售日报-今年零售合计计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df4 = pd.read_sql(sql=sql_201910retail, con=engine)
    write_excel(df=df4, sheet_name='去年')
    zl.logger.warning("零售日报-去年零售合计计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df5 = pd.read_sql(sql=sql_mmp2, con=engine)
    write_excel(df=df5, sheet_name='MMP')
    zl.logger.warning("指环王-MMP零售数据计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df6 = pd.read_sql(sql=sql_mmp3, con=engine)
    write_excel(df=df6, sheet_name='渠道分类')
    zl.logger.warning("指环王-渠道分类计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df7 = pd.read_sql(sql=sql_mmp4, con=engine)
    write_excel(df=df7, sheet_name='代理渠道')
    zl.logger.warning("指环王-代理渠道计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df8 = pd.read_sql(sql=sql_202010top3c, con=engine)
    write_excel(df=df8, sheet_name='全月')
    zl.logger.warning("零售日报-全月零售数据计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    df9 = pd.read_sql(sql=sql_202010date, con=engine)
    write_excel(df=df9, sheet_name='近四天')
    zl.logger.warning("零售日报-近四天零售数据计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df10 = pd.read_sql(sql=sql_channel_1, con=engine)
    # write_excel(df=df10, sheet_name='今年代理渠道销售')
    # zl.logger.warning("零售日报-今年代理渠道销售计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df11 = pd.read_sql(sql=sql_channel_2, con=engine)
    # write_excel(df=df11, sheet_name='去年代理渠道销售')
    # zl.logger.warning("零售日报-去年代理渠道销售计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df12 = pd.read_sql(sql=sql_channel_3, con=engine)
    # write_excel(df=df12, sheet_name='上个月代理渠道销售')
    # zl.logger.warning("零售日报-上个月代理渠道销售计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df13 = pd.read_sql(sql=sql_mmp_last_month, con=engine)
    # write_excel(df=df13, sheet_name='上个月mmp销售')
    # zl.logger.warning("零售日报-上个月mmp销售计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df14 = pd.read_sql(sql=sql_colmo, con=engine)
    # write_excel(df=df14, sheet_name='colmo当月销售')
    # zl.logger.warning("指环王-colmo当月销售，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df15 = pd.read_sql(sql=sql_19mmp_retail, con=engine)
    # write_excel(df=df15, sheet_name='19年MMP')
    # zl.logger.warning("指环王-19年MMP计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #
    # df16 = pd.read_sql(sql=sql_19channel_retail, con=engine)
    # write_excel(df=df16, sheet_name='19年代理渠道')
    # zl.logger.warning("指环王-19年代理渠道计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #
    # df17 = pd.read_sql(sql=sql_quniantongqilingshou, con=engine)
    # write_excel(df=df17, sheet_name='去年零售同期')
    # zl.logger.warning("去年零售同期计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #
    # df18 = pd.read_sql(sql=sql_jinnianlingshou, con=engine)
    # write_excel(df=df18, sheet_name='今年零售')
    # zl.logger.warning("今年零售计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df19 = pd.read_sql(sql=sql_colmozhuangxiang, con=engine)
    # write_excel(df=df19, sheet_name='colmo专项')
    # zl.logger.warning("全球购物狂欢节-colmo专项计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # df20 = pd.read_sql(sql=sql_taikongcangzhuangxiang, con=engine)
    # write_excel(df=df20, sheet_name='太空舱专项')
    # zl.logger.warning("全球购物狂欢节-太空舱专项计算完毕，结束计算时间 :"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    writer.save()
    zl.logger.warning("已完成，时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

if __name__ == '__main__':
    export_data()