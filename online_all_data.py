import module_import_data as imd
import pandas as pd
import pymysql
from sqlalchemy import create_engine

class AllOLData:
    def __init__(self):
        self.conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.')
        self.engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306")
        #苏宁20年日销
        self.file_sn_daily_sale= r'\\10.157.2.94\共享文件\Data\线上数据\20年数据\SN20年日销.xlsx'
        self.table_sn_daily_sale_result="""
        select date(统计日期) as 日期 , 品牌名称, '苏宁' as 一级分类, 业务渠道 as 二级分类, 型号, sum(销售数量) as 销售数量, sum(销售金额) as 销售金额
        from ods.线上苏宁原始数据
        group by 统计日期, 品牌名称, 业务渠道, 型号 ;
        """
        self.file_tmall_daily_sale=r'\\10.157.2.94\共享文件\Data\线上数据\20年数据\TM各渠道分型号每月销售-20年12月.xlsx'
        self.table_tmall_daily_sale_result="""
        SELECT date(月份) as 日期,品牌 as 品牌名称,'天猫淘宝'as 一级分类,店铺 as 二级分类, 型号 , sum(销量) as 销售数量, sum(销售额) as 销售金额
        FROM ods.线上天猫淘宝原始数据
        group by 月份, 品牌, 店铺, 型号;
        """
        self.file_new_channel_sale=r'\\10.157.2.94\共享文件\Data\线上数据\20年数据\新渠道20年数据明细汇总-简.xlsx'
        self.table_new_channel_sale_result="""
        SELECT date(支付日期) as 日期,品牌1 as 品牌名称,'新渠道'as 一级分类,`21年新分类` as 二级分类, 型号 , sum(数量) as 销售数量, sum(金额) as 销售金额
        FROM ods.线上新渠道原始数据
        group by 支付日期, 品牌, `21年新分类`, 型号;
        """
        self.file_long_tail_sale = r'\\10.157.2.94\共享文件\Data\线上数据\20年数据\长尾各渠道分型号每月销售-长尾2016至2020年11月.xlsx'
        self.table_long_tail_sale_result="""
        select date(月份) as 日期, 品牌 as 品牌名称, '线上长尾' as 一级分类, `标准化店名` as 二级分类, 型号 , sum(销量) as 销售数量, sum(销售额) as 销售金额
        from ods.线上长尾原始数据
        group by 月份, 品牌, `标准化店名`, 型号;
        """

    def close_con(self):
        self.conn.close()

    def sn_data_init(self):
        try:
            df = pd.read_excel(self.file_sn_daily_sale, sheet_name='原数据（每日更新）', header=0,parse_dates = ['统计日期'])
            imd.df2db(df,'线上苏宁原始数据','replace',self.engine,'ods')
        except Exception as e:
            print('Error and reason: ')
            print(e)

    def part_of_sn(self):
        try:
            df_sn = pd.read_sql(sql=self.table_sn_daily_sale_result, con=self.engine)
        except Exception as e:
            print('Error and reason: ')
            print(e)
        return df_sn

    def tmall_data_init(self):
        try:
            df = pd.read_excel(self.file_tmall_daily_sale, sheet_name='分型号日', header=1, parse_dates=['月份'])
            imd.df2db(df, '线上天猫淘宝原始数据', 'replace', self.engine, 'ods')
        except Exception as e:
            print('Error and reason: ')
            print(e)

    def part_of_tmall(self):
        try:
            df_tmall = pd.read_sql(sql=self.table_tmall_daily_sale_result, con=self.engine)
        except Exception as e:
            print('Error and reason: ')
            print(e)
        return df_tmall

    def new_channel_data_init(self):
        try:
            df = pd.read_excel(self.file_new_channel_sale, sheet_name='20年数据', header=0, parse_dates=['支付日期'])
            imd.df2db(df, '线上新渠道原始数据', 'replace', self.engine, 'ods')
        except Exception as e:
            print('Error and reason: ')
            print(e)

    def part_of_new_channel(self):
        try:
            df_new_channel = pd.read_sql(sql=self.table_new_channel_sale_result, con=self.engine)
        except Exception as e:
            print('Error and reason: ')
            print(e)
        return df_new_channel

    def long_tail_data_init(self):
        try:
            df = pd.read_excel(self.file_long_tail_sale, sheet_name='原始数据', header=0, parse_dates=['月份'])
            imd.df2db(df, '线上长尾原始数据', 'replace', self.engine, 'ods')
        except Exception as e:
            print('Error and reason: ')
            print(e)

    def part_of_long_tail(self):
        try:
            df = pd.read_sql(sql=self.table_long_tail_sale_result, con=self.engine)
        except Exception as e:
            print('Error and reason: ')
            print(e)
        return df

if __name__ == '__main__':
    obj1=AllOLData()
    # obj1.sn_data_init()#初始化苏宁数据
    # obj1.part_of_sn()#返回苏宁部分数据
    # obj1.tmall_data_init()#初始化天猫淘宝数据
    # obj1.part_of_tmall()#返回天猫淘宝数据
    # obj1.new_channel_data_init()#初始化新渠道数据
    # obj1.part_of_new_channel()#返回新渠道数据
    # obj1.long_tail_data_init()#初始化新渠道数据
    # obj1.part_of_long_tail()#返回长尾数据
