import pandas as pd
from sqlalchemy import create_engine

def update_dry_all_data():
    # 创建数据库连接
    engine_read = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306")
    # 源数据文件路径、sql
    # 定义读取门店系统配置
    path_store_system = r'D:\MyData\ex_chenyj12\Documents\001 文档\鹅小天\干衣机系统\中心系统门店匹配.xlsx'
    df_store_system = pd.read_excel(path_store_system, sheet_name='Sheet1', header=0)
    #
    df_store_system.to_sql('中心客户门店配置', con=engine_read,schema='dim', if_exists='replace', index=False)
    # 读取Excel干衣机供价信息
    path_model_price = r'D:\MyData\ex_chenyj12\Documents\001 文档\鹅小天\干衣机系统\干衣机编码型号供价.xlsx'
    df_model_price = pd.read_excel(path_model_price, sheet_name='Sheet1', header=0)

    path_center_type = r'D:\MyData\ex_chenyj12\Documents\001 文档\鹅小天\干衣机系统\中心类别匹配.xlsx'
    df_center_type = pd.read_excel(path_center_type, sheet_name='Sheet1', header=0)

    sql_dryer_retail=""" select  商品型号, 商品编码,  门店编码, 门店名称, 门店等级, 门店一级分类, 门店二级分类, b.中心, 创建时间, sum(数量) as 销量
    from ods.mmp零售数据全量 a
    inner join dim.中心分部配置 b 
    on a.分部名称 =b.分部名称 
    where a.门店等级 is not null 
    group by 商品型号, 商品编码, 门店编码, 门店名称, 门店等级, 门店一级分类, 门店二级分类, b.中心 , 产品线, 创建时间 """
    sql_cmdm_store="""SELECT distinct a.门店名称,a.门店编码,a.门店等级, 一级分类, 二级分类
    FROM ods.终端门店数据 a
    inner join dim.中心分部配置 b 
    on b.分部名称 = a.所属分部
    inner join dim.零售门店分类 c
    on c.门店一级分类 = a.一级分类 
    and c.门店二级分类 =a.二级分类 
    where a.经营状态='经营中'
    and a.门店等级 is not null
    and c.类别 in ('3c','top')"""
    sql_day7_dryer_retail="""  select  商品型号, 商品编码,  门店编码, 门店名称, 门店等级, b.中心, sum(数量) as 销量
    from ods.mmp零售数据全量 a
    inner join dim.中心分部配置 b 
    on a.分部名称 =b.分部名称 
    where ods.当月月累(创建时间)
    group by 商品型号, 商品编码, 门店编码, 门店名称, 门店等级, b.中心  """
    sql_mmp_retail="""select
       b.中心,品牌,商品型号, 商品编码, 门店编码, 门店名称, 门店等级, 门店二级分类, 创建时间,
       case when 门店一级分类 not in ('国美','苏宁','五星') then 'TOP直营' else 门店一级分类 end as 门店一级分类,
       sum(a.数量 * c.系数) as 销量,
       sum(a.总价)/10000 as 销额
    from
       ods.mmp零售数据全量 a
    inner join ods.干衣机型号配置表 c on 
       a.商品编码 = c.编号
    inner join dim.中心分部配置 b on
       a.分部名称 = b.分部名称
    where
       ((a.门店一级分类 in ('苏宁',
       '国美',
       '五星',
       'TOP')
       and
       a.门店二级分类 not in ('国美新零售', '苏宁零售云', '五星万镇通'))
       or a.门店编码 in ('S00000353','S00001776','S00001847','S00002118','S00002119','S00003077','S00003719','S00003773','S00003928','S00003986','S00003990','S00005033','S00000752','S00001777','S00003707','S00005206','S00005257','S00005368','S00005729','S00081607','S00081632','S00081635','S00081648','S00082093','S00091085','S00215530','S00089426','S00081003','S00081004','S00087008','S00191565','S00204520','S00081031','S00081155','S00090191','S00195186','S00081080','S00081541','S00090768','S00090769','S00090774','S00048922','S00084355','S00084376','S00084392','S00084393','S00081818','S00156712','S00084600','S00084648','S00001939','S00002627','S00003872','S00005312','S00078945','S00081418','S00081424','S00081437','S00095389','S00068629','S00068639','S00013902','S00280139','S00036059','S00036060','S00076764','S00102010','S00081706','S00203272','S00081227','S00081228','S00083873','S00014418','S00084297','S00084755','S00088712','S00095896','S00081376','S00081383','S00081407','S00121787','S00181472','S00239250','S00081394','S00081411','S00111482','S00095292','S00081701','S00082875','S00252523','S00402832','S00081628','S00081695','S00081702','S00081714','S00081717','S00082886','S00253325','S00260399','S00270654','S00433375','S00023693','S00005258')
       )
    group by
       b.中心,品牌,
       a.门店一级分类,商品型号, 商品编码, 门店编码, 门店名称, 门店等级, 门店二级分类, 创建时间;"""
    # 读取数据

    df_dryer_retail = pd.read_sql(sql_dryer_retail, con=engine_read)
    df_cmdm_store = pd.read_sql(sql_cmdm_store, con=engine_read)
    df_day7_dryer_retail = pd.read_sql(sql_day7_dryer_retail, con=engine_read)
    df_mmp_retail = pd.read_sql(sql_mmp_retail, con=engine_read)

    df_retail_result=df_dryer_retail.merge(df_store_system[['门店编码','客户简称','中心类别']],how='inner',on='门店编码',left_index=False,right_index=False)\
    .merge(df_model_price[['品牌','商品编码','系数']],how='inner',on='商品编码',left_index=False,right_index=False)

    df_mmp_retail_result=df_mmp_retail.merge(df_store_system[['门店编码','客户简称']],how='left',on='门店编码',left_index=False,right_index=False).fillna('其他')\
    .merge(df_center_type[['中心','中心类别']],how='inner',on='中心',left_index=False,right_index=False)

    # df_retail_result.to_sql('mmp零售干衣机明细',engine_read,schema='dwd',if_exists='replace',index=True,index_label='序号')
    df_mmp_retail_result.to_sql('mmp零售干衣机明细',engine_read,schema='dwd',if_exists='replace',index=True,index_label='序号')

    df_day7_dryer_retail_result=df_day7_dryer_retail.merge(df_model_price[['品牌','商品编码']],how='inner',on='商品编码',left_index=False,right_index=False)

    df_zero_retail_store=df_cmdm_store.merge(df_store_system[['中心','门店编码','客户简称','中心类别']],how='inner',on='门店编码',left_index=False,right_index=False)\
    .merge(df_day7_dryer_retail_result[['品牌','门店编码']],how='left',on='门店编码',left_index=False,right_index=False).fillna('各品牌均无销售')

    df_zero_retail_store.to_sql('mmp零售干衣机0销售门店',engine_read,schema='dwd',if_exists='replace',index=True,index_label='序号')

    df_cmdm_store.merge(df_store_system[['中心','门店编码','客户简称','中心类别']],how='inner',on='门店编码',left_index=False,right_index=False)\
    .merge(df_day7_dryer_retail_result[['品牌','门店编码']],how='left',on='门店编码',left_index=False,right_index=False).fillna('各品牌均无销售')

    # 分品牌处理未销售数据源
    df_all_zero_retail=pd.DataFrame(df_zero_retail_store[df_zero_retail_store['品牌']=='各品牌均无销售'])
    df_colmo_zero_retail=pd.DataFrame(df_zero_retail_store[df_zero_retail_store['品牌']!='COLMO'])
    df_colmo_zero_retail.loc[:,'品牌']='COLMO未销售'
    df_colmo_zero_retail.drop_duplicates(keep='first',inplace=True)
    df_ls_zero_retail=pd.DataFrame(df_zero_retail_store[df_zero_retail_store['品牌']!='小天鹅'])
    df_ls_zero_retail.loc[:,'品牌']='小天鹅未销售'
    df_ls_zero_retail.drop_duplicates(keep='first',inplace=True)
    df_md_zero_retail=pd.DataFrame(df_zero_retail_store[df_zero_retail_store['品牌']!='美的'])
    df_md_zero_retail.loc[:,'品牌']='美的未销售'
    df_md_zero_retail.drop_duplicates(keep='first',inplace=True)

    df_result=pd.concat([df_colmo_zero_retail, df_ls_zero_retail,df_md_zero_retail,df_all_zero_retail])
    df_result.to_sql('mmp零售干衣机分品牌0销售',engine_read,schema='dwd',if_exists='replace',index=True,index_label='序号')
    print("干衣机数据更新已完成")

if __name__ == '__main__':
    update_dry_all_data()