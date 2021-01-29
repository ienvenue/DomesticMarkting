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
        select date(统计日期) as 日期 , 品牌名称, '苏宁' as 一级分类, 业务渠道 as 二级分类, 商品编码, 型号, sum(销售数量) as 销售数量, sum(销售金额) as 销售金额
        from ods.线上苏宁原始数据
        group by 统计日期, 品牌名称, 业务渠道, 商品编码, 型号 ;
        """

    def close_con(self):
        self.conn.close()

    def sn_data_init(self):
        try:
            df = pd.read_excel(self.file_sn_daily_sale, sheet_name='原数据（每日更新）', header=0,parse_dates = ['统计日期'])
            imd.df2db(df,'线上苏宁原始数据','replace',self.engine,'ods')
        except Exception as e:
            print(e)

    def part_of_sn(self):
        df_sn=pd.read_sql(sql=self.table_sn_daily_sale_result,con=self.engine)
        return df_sn

if __name__ == '__main__':
    obj1=AllOLData()
    obj1.sn_data_init()#初始化苏宁数据
    obj1.part_of_sn()#返回苏宁部分数据
