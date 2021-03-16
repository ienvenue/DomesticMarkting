import pymysql
import time
import dryer_mmp_data as dd

class AVCUpdate:
    def __init__(self):
        self.conn = pymysql.connect(host='10.157.2.94', user='data_dev', password='data_dev0.')
        self.update_month_data = '''call dwd.奥维月度数据汇总; '''
        self.update_week_data = ''' call dwd.奥维周度数据汇总; '''
        self.update_model_data = ''' call dwd.奥维含机型数据更新; '''
        self.data_update_time=''' call dwd.数据更新时间; '''
        self.update_offline_all=''' call `dwd`.`线下全渠道数据`; '''

    def close_con(self):
        self.conn.close()

    def update_offline_all_data(self):
        cur = self.conn.cursor()
        # 更新dwd 线下全渠道数据
        cur.execute(self.update_offline_all)
        cur.close()
        self.conn.commit()
        print("更新线下全渠道数据时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def update_group_month_data(self):
        cur = self.conn.cursor()
        # 更新dwd schema下的所有奥维月度XXXXX汇总表
        cur.execute(self.update_month_data)
        cur.close()
        self.conn.commit()
        print("更新dwd schema下的所有奥维月度XXXXX汇总表时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def update_group_week_data(self):
        cur = self.conn.cursor()
        # 更新dwd schema下的所有奥维周度XXXXX汇总表
        cur.execute(self.update_week_data)
        cur.close()
        self.conn.commit()
        print("更新dwd schema下的所有奥维周度XXXXX汇总表时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def update_group_model_data(self):
        cur = self.conn.cursor()
        # 更新dwd schema下的所有奥维机型汇总表
        cur.execute(self.update_model_data)
        cur.close()
        self.conn.commit()
        print("更新dwd schema下的所有奥维机型汇总表时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def update_data_update_time(self):
        cur = self.conn.cursor()
        # 更新数据监控时间
        cur.execute(self.data_update_time)
        cur.close()
        self.conn.commit()
        print("更新数据监控时间时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def update_standard_month_data(self, start_time, last_month, this_month, last_month_week, this_month_week):
        """
        更新dwd.标准化月度XXXXX表数据
        :param start_time:
        :param last_month:
        :param this_month:
        :param last_month_week:
        :param this_month_week:
        :return:
        """
        cur = self.conn.cursor()
        # 每周每月新数据都需要修改 【周度 in ('19W49', '19W50', '19W51', '19W52')(月度 + 0 >= 19)】
        drop_dwd_标准化月度线下洗衣机 = ''' drop  table if exists dwd.标准化月度线下洗衣机;  '''
        create_dwd_标准化月度线下洗衣机 = '''
            create table dwd.标准化月度线下洗衣机 as
            select 销量,销额,aowei.奥维月份标准化(时间) as 时间, 品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新 from (
            # 开始时间年全年数据+20年1-11月数据
            SELECT sum(销量) as 销量, sum(销额) as 销额, 月度 as 时间, 品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            FROM dwd.月度线下洗衣机汇总
            where (月度+0>={})
            group by 月度,品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            union all 
            # 用21年的周度数据拼出21年1月数据
            SELECT sum(销量) as 销量, sum(销额) as 销额, '{}', 品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            FROM dwd.周度线下洗衣机汇总
            where 周度 in ({})
            group by 周度,品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            union all 
            # 用20年的周度数据拼出20年12月数据
            SELECT sum(销量) as 销量, sum(销额) as 销额, '{}', 品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            FROM dwd.周度线下洗衣机汇总
            where 周度 in ({})
            group by 周度,品牌, 洗涤类型, 子品牌, 美的渠道, 洗衣机美的中心, 通用价格段新
            ) a;
            '''.format(start_time, this_month, this_month_week, last_month, last_month_week)
        drop_dwd_标准化月度线下干衣机 = ''' drop  table if exists dwd.标准化月度线下干衣机;  '''
        create_dwd_标准化月度线下干衣机 = """
        create table dwd.标准化月度线下干衣机 as
        select 销量,销额,aowei.奥维月份标准化(时间) as 时间, 品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段,线下容量段 from (
        # 19年全年数据+20年1-11月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, 月度 as 时间, 品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段,线下容量段
        FROM dwd.月度线下干衣机汇总
        where (月度+0>={})
        group by 月度,品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段
        union all 
        # 用20年的周度数据拼出20年12月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}', 品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段,线下容量段
        FROM dwd.周度线下干衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段
        union all 
        # 用21年的周度数据拼出21年1月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}', 品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段,线下容量段
        FROM dwd.周度线下干衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 干衣方式, 子品牌, 美的渠道, 洗衣机美的中心, 线下价格段) a;
        """.format(start_time, this_month, this_month_week, last_month, last_month_week)
        drop_dwd_标准化月度线上干衣机 = ''' drop  table if exists dwd.标准化月度线上干衣机;  '''
        create_dwd_标准化月度线上干衣机 = """
        create table dwd.标准化月度线上干衣机 as
        select 销量,销额,aowei.奥维月份标准化(时间) as 时间, 品牌, 干衣方式, 子品牌, 电商1 as 电商渠道,通用价格段, 线上价格段 from (
        # 19年全年数据+20年1-11月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, 月度 as 时间, 品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段
        FROM dwd.月度线上干衣机汇总
        where (月度+0>={})
        group by 月度,品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段
        union all 
        # 用20年的周度数据拼出20年12月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}',品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段
        FROM dwd.周度线上干衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段 
        union all 
        # 用21年的周度数据拼出21年1月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}',品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段
        FROM dwd.周度线上干衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 干衣方式, 子品牌, 电商1,通用价格段, 线上价格段 ) a;
        """.format(start_time, this_month, this_month_week, last_month, last_month_week)
        drop_dwd_标准化月度线上洗衣机 = '''drop  table if exists dwd.标准化月度线上洗衣机; '''
        create_dwd_标准化月度线上洗衣机 = """
        create table dwd.标准化月度线上洗衣机 as
        select 销量,销额,aowei.奥维月份标准化(时间) as 时间, 品牌, 洗涤类型, 子品牌, 电商1 as 电商渠道,价格段 from (
        # 19年全年数据+20年1-11月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, 月度 as 时间, 品牌, 洗涤类型, 子品牌, 电商1,价格段
        FROM dwd.月度线上洗衣机汇总
        where (月度+0>={})
        group by 月度,品牌, 洗涤类型, 子品牌, 电商1,价格段
        union all 
        #  用20年的周度数据拼出20年12月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}',品牌, 洗涤类型, 子品牌, 电商1,价格段
        FROM dwd.周度线上洗衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 洗涤类型, 子品牌, 电商1,价格段
        union all 
        #  用21年的周度数据拼出21年1月数据
        SELECT sum(销量) as 销量, sum(销额) as 销额, '{}',品牌, 洗涤类型, 子品牌, 电商1,价格段
        FROM dwd.周度线上洗衣机汇总
        where 周度 in ({})
        group by 周度,品牌, 洗涤类型, 子品牌, 电商1,价格段) a;
        """.format(start_time, this_month, this_month_week, last_month, last_month_week)
        # print(drop_dwd_标准化月度线下洗衣机)
        # print(create_dwd_标准化月度线下洗衣机)
        # print(drop_dwd_标准化月度线下干衣机)
        # print(create_dwd_标准化月度线下干衣机)
        # print(drop_dwd_标准化月度线上干衣机)
        # print(create_dwd_标准化月度线上干衣机)
        # print(drop_dwd_标准化月度线上洗衣机)
        # print(create_dwd_标准化月度线上洗衣机)
        cur.execute(drop_dwd_标准化月度线下洗衣机)
        cur.execute(create_dwd_标准化月度线下洗衣机)
        cur.execute(drop_dwd_标准化月度线下干衣机)
        cur.execute(create_dwd_标准化月度线下干衣机)
        cur.execute(drop_dwd_标准化月度线上干衣机)
        cur.execute(create_dwd_标准化月度线上干衣机)
        cur.execute(drop_dwd_标准化月度线上洗衣机)
        cur.execute(create_dwd_标准化月度线上洗衣机)
        print("标准化月度数据更新时间 :" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        cur.close()
        self.conn.commit()
        
def avc_month_data_update():
    obj = AVCUpdate()
    obj.update_group_month_data()  # 更新奥维月度数据
    obj.update_standard_month_data(19, "", "", "'None'", "'None'")  # 不含周度 标准化月度更新(开始时间,上个月,本月,上个月的周,本月周)
    obj.update_data_update_time()  # 更新数据更新时间

def avc_week_data_update():
    obj = AVCUpdate()
    obj.update_group_week_data()  # 更新奥维周度数据
    obj.update_group_model_data()  # 更新奥维机型数据
    obj.update_data_update_time()  # 更新数据更新时间

if __name__ == '__main__':
    obj1 = AVCUpdate()
    # obj1.update_group_month_data()#更新奥维月度数据
    # obj1.update_group_week_data()#更新奥维周度数据
    # obj1.update_group_model_data()#更新奥维机型数据
    # 暂时不使用 obj1.update_standard_month_data(19, "", "21.01", "'None'", "'21W01','21W02','21W03','21W04','21W05'") #含周度 标准化月度更新(开始时间,上个月,本月,上个月的周,本月周)
    # obj1.update_standard_month_data(19, "", "", "'None'", "'None'") #不含周度 标准化月度更新(开始时间,上个月,本月,上个月的周,本月周)
    # obj1.update_data_update_time()#更新数据更新时间
    obj1.update_offline_all_data()#更新线下全渠道数据
    dd.update_dry_all_data()#干衣机数据更新
    obj1.close_con()#关闭数据库连接