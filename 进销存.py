import time

import pandas as pd
import pymysql
from sqlalchemy import create_engine

import import_retail_data as imd


class Invoicing:
    def __init__(self):
        # 导入数据文件地址
        self.path_inner_center_inventory = r'\\10.157.2.94\共享文件\Data\进销存方案\【国内市场】2021年2月23日洗衣机库存日报表.xlsx'
        self.path_all_order = r'\\10.157.2.94\共享文件\Data\进销存方案\系统未审21-02-24.xlsx'
        # 导入数据库链接
        self.conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.')
        self.engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306")
        # 当前时间字符串
        self.date_str = str(time.strftime("%Y-%m-%d", time.localtime()))

        self.this_month_retail = ''' select 中心, a.品牌, 一级分类,二级分类, b.产品型号, 商品编码,
        case when 二级分类 in ('一级代理渠道','二级代理渠道') then '代理' when 二级分类 in ('TOP') then '直营' else 二级分类 end as 系统, sum(销量) 销量
        from dwd.线下全渠道数据 a
        inner join dim.机型配置表 b on
        b.编号 = a.商品编码
        where ods.当月月累(日期)
        group by 中心, a.品牌, 一级分类,二级分类, b.产品型号, 商品编码; '''
        self.this_year_retail = ''' select 中心, a.品牌, 一级分类,二级分类, b.产品型号, 商品编码,
        case when 二级分类 in ('一级代理渠道','二级代理渠道') then '代理' when 二级分类 in ('TOP') then '直营' else 二级分类 end as 系统, sum(销量) 销量
        from dwd.线下全渠道数据 a
        inner join dim.机型配置表 b on
        b.编号 = a.商品编码
        where year(日期)= '2021'
        group by 中心, a.品牌, 一级分类,二级分类, b.产品型号, 商品编码;'''
        self.inner_center_inventory = '''  select a.中心, a.仓库类型, b.品牌, b.编号 , b.产品型号, sum(a.现有量) as 数量
                                                from ods.洗内内部库存明细 a
                                                inner join dim.机型配置表 b on
                                                b.编号 = a.产品编码
                                                where a.仓库大类 = '中心仓'
                                                and a.仓库类型 in ('正品', '在途')
                                                group by a.中心, a.仓库类型, b.品牌, b.编号, b.产品型号; '''
        self.sql_order = '''(select a.划拨中心 as 中心, '开单未提' as 订单状态, case when a.单据类型 = '正常调拨' then '调拨单'
        else a.单据类型 end as 订单类型 , b.品牌, b.编号 , b.产品型号
        ,case when 性质='线下长尾' then '直营' when 性质='苏宁线下' then '苏宁' when 性质='超市' then '代理' else 性质 end as 系统, sum(a.未发数量) as 数量
        from ods.洗内订单开提明细 a
        inner join dim.机型配置表 b on
        b.编号 = a.产品编码
        inner join (select distinct 中心
        from dim.中心分部配置) c on
        c.中心 = a.划拨中心
        where 单据类型 in ('正常调拨', '销售单')
        group by a.划拨中心, a.单据类型, b.品牌, b.编号,性质 , b.产品型号)
        union all (select a.中心, case when a.是否结转 = '是' then '已结转'
        else '未结转' end as 订单状态, a.订单类型, b.品牌, b.编号 , b.产品型号
        ,case when 性质='线下长尾' then '直营' when 性质='苏宁线下' then '苏宁' when 性质='超市' then '代理' else 性质 end as 系统, sum(a.数量) as 数量
        from ods.洗内订单未审明细 a
        inner join dim.机型配置表 b on
        b.编号 = a.产品编码
        inner join (select distinct 中心
        from dim.中心分部配置) c on
        c.中心 = a.中心
        where 订单类型 in ('调拨单', '销售单')
        group by a.中心, a.订单类型, a.是否结转, b.品牌, b.编号 , b.产品型号,性质);'''
        self.fahuo_goods = ''' select c.中心, b.品牌, b.产品型号, b.编号 as 商品编码, sum(a.产品数量 * b.系数) as 销量
                                    from ods.发货数据明细 a
                                    inner join dim.机型配置表 b on
                                    substring_index(substring_index(产品编码, '"', 2), '"',-1) = b.编号
                                    inner join ods.中心分部配置 c on
                                    a.营销中心名称 = c.分部名称
                                    where ods.当月月累(单据日期)
                                    group by c.中心, b.品牌, b.产品型号, b.编号;        '''

    def read_data_from_xlsx(self):
        df1 = pd.read_excel(self.path_inner_center_inventory, sheet_name='库存明细', header=0)
        imd.add_today_column(df1, self.date_str)
        imd.save_data(df1, '洗内内部库存明细', 'replace')
        df2 = pd.read_excel(self.path_all_order, sheet_name='未审', header=1)
        imd.add_today_column(df2, self.date_str)
        imd.save_data(df2, '洗内订单未审明细', 'replace')
        df3 = pd.read_excel(self.path_all_order, sheet_name='开提', header=1)
        imd.add_today_column(df3, self.date_str)
        df3.rename(columns={"单据状态 ": "单据状态", "实际发货数量 ": "实际发货数量"}, inplace=True)
        imd.save_data(df3, '洗内订单开提明细', 'replace')

    def pivot_result_from_sql(self):
        # 当月销售
        df_this_month_retail = pd.read_sql(self.this_month_retail, self.engine)
        df_this_month_retail_result = df_this_month_retail.pivot_table(values=["销量"], index=["中心", "品牌", "产品型号"],
                                                                       columns=["一级分类"], aggfunc='sum', fill_value=0)
        # 当年销售
        df_this_year_retail = pd.read_sql(self.this_year_retail, self.engine)
        df_this_year_retail_result = df_this_year_retail.pivot_table(values=["销量"], index=["中心", "品牌", "产品型号"],
                                                                     columns=["一级分类"], aggfunc='sum', fill_value=0)
        # 中心仓库存
        df_inner_center_inventory = pd.read_sql(self.inner_center_inventory, self.engine)
        df_inner_center_inventory_result = df_inner_center_inventory.pivot_table(values=["数量"],
                                                                                 index=["中心", "品牌", "产品型号"],
                                                                                 columns=["仓库类型"], aggfunc='sum',
                                                                                 fill_value=0)
        # 系统订单
        df_sql_order = pd.read_sql(self.sql_order, self.engine)
        df_sql_order_result = df_sql_order.pivot_table(values=["数量"], index=["中心", "品牌", "产品型号"],
                                                       columns=["订单类型", "订单状态"], aggfunc='sum', fill_value=0)
        # 当月提货
        df_sql_fahuo = pd.read_sql(self.fahuo_goods, self.engine)
        df_sql_fahuo_result = df_sql_fahuo.pivot_table(values=["销量"], index=["中心", "品牌", "产品型号"], aggfunc='sum',
                                                       fill_value=0)
        all_result = df_this_year_retail_result.merge(df_this_month_retail_result, how='left', left_index=True,
                                                      right_index=True) \
            .merge(df_inner_center_inventory_result, how='left', left_index=True, right_index=True) \
            .merge(df_sql_order_result, how='left', left_index=True, right_index=True) \
            .merge(df_sql_fahuo_result, how='left', left_index=True, right_index=True).fillna(0)
        all_result.reset_index(inplace=True)
        all_result.columns = ['中心', '品牌', '产品型号', 'TOP年累销售', '线下运营年累销售',
                              '连锁年累销售', 'TOP月累销售', '线下运营月累销售', '连锁月累销售', '在库中心仓库存', '在途中心仓库存'
            , '开单未提调拨单', '已结转调拨单', '未结转调拨单', '开单未提销售单', '已结转销售单', '未结转销售单',
                              '提货台数']
        all_result.to_sql(name='干衣机货源保障跟踪', schema='dwd', con=self.engine, if_exists='replace')

    def pivot_system_from_sql(self):
        # 当月销售
        df_this_month_retail = pd.read_sql(self.this_month_retail, self.engine)
        df_this_month_retail_result = df_this_month_retail.pivot_table(values=["销量"], index=["中心", "品牌", "产品型号", "系统"],
                                                                       aggfunc='sum', fill_value=0)
        # 当年销售
        df_this_year_retail = pd.read_sql(self.this_year_retail, self.engine)
        df_this_year_retail_result = df_this_year_retail.pivot_table(values=["销量"], index=["中心", "品牌", "产品型号", "系统"],
                                                                     aggfunc='sum', fill_value=0)
        # 系统订单
        df_sql_order = pd.read_sql(self.sql_order, self.engine)
        df_sql_order_result = df_sql_order.pivot_table(values=["数量"], index=["中心", "品牌", "产品型号", "系统"],
                                                       columns=["订单类型", "订单状态"], aggfunc='sum', fill_value=0)

        all_result = df_this_year_retail_result.merge(df_this_month_retail_result, how='left', left_index=True,
                                                      right_index=True) \
            .merge(df_sql_order_result, how='left', left_index=True, right_index=True).fillna(0)
        all_result.reset_index(inplace=True)
        all_result.columns = ['中心', '品牌', '产品型号', '系统', '年累销售', '月累销售', '开单未提调拨单', '已结转调拨单', '未结转调拨单', '开单未提销售单',
                              '已结转销售单', '未结转销售单']
        all_result.to_sql(name='干衣机进销跟踪', schema='dwd', con=self.engine, if_exists='replace')


if __name__ == '__main__':
    obj1 = Invoicing()
    obj1.read_data_from_xlsx()
    obj1.pivot_result_from_sql()
    obj1.pivot_system_from_sql()
