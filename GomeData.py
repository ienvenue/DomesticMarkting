from datetime import datetime
from win32com.client import Dispatch
import time,re
import pandas as pd
from sqlalchemy import create_engine

def remodel(ser_str):
    """
    使用正则表达式替换【商品 - 长文本】的汉字，替换为空
    :param ser_str: 【商品 - 长文本】
    :return: 替换好的【商品型号】
    """
    pattern = re.compile('[\u4e00-\u9fa5]{0,}')
    return re.sub(pattern, "", ser_str)

def inventory2db(path, tablename, sheetname, type, cols, rownum):
    """
    读出excel数据，并按如下清洗数据
    1.替换商品型号中的汉字
    2.重命名列名
    :param path:excel文件路径
    :param tablename:数据库表名
    :param sheetname:Sheet页名
    :param type:导入类型 replace or append
    :param cols:字段名
    :param rownum:开始行数
    :return:无
    """
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    # 库存需要注释
    # df = df[(df['23.销售数量']!=0)]
    # 替换商品型号中的汉字
    df['商品型号']=df['商品 - 长文本'].map(remodel)
    df=df.drop('商品 - 长文本',axis=1)
    # 重命名列名
    df.rename(columns={"日历天": "时间", "商品 - 键值": "商品编码", "23.销售数量": "销量", "销售净额": "销额","3 销售净额（含促销补差）": "销额",
                       "库位标识（正品、残次、样机及赠品）":"库位标识"}, inplace=True)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)

def retail2db(path, tablename, sheetname, type, cols, rownum):
    """
    读出excel数据，并按如下清洗数据
    0.过滤销售量为0的垃圾数据
    1.替换商品型号中的汉字
    2.重命名列名
    :param path:excel文件路径
    :param tablename:数据库表名
    :param sheetname:Sheet页名
    :param type:导入类型 replace or append
    :param cols:字段名
    :param rownum:开始行数
    :return:无
    """
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    # 库存需要注释
    df = df[(df['23.销售数量']!=0)]
    # 替换商品型号中的汉字
    df['商品型号']=df['商品 - 长文本'].map(remodel)
    df=df.drop('商品 - 长文本',axis=1)
    # 重命名列名
    df.rename(columns={"日历天": "时间", "商品 - 键值": "商品编码", "23.销售数量": "销量", "销售净额": "销额","3 销售净额（含促销补差）": "销额",
                       "库位标识（正品、残次、样机及赠品）":"库位标识"}, inplace=True)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)

def csv2db(path, tablename, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_csv(path, header=rownum, usecols=cols, encoding="gbk")
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

def csv2xlsx(path_weishen_csv, path_weishen_xlsx, cols):
    csv_temp_df = pd.read_csv(path_weishen_csv, header=1, usecols=cols, encoding="gbk")
    csv_temp_df.to_excel(path_weishen_xlsx, sheet_name='temp')
    app = Dispatch("Excel.Application")
    app.Visible = False
    content = app.Workbooks.Open(path_weishen_xlsx)
    content.Save()
    content.Close()


if __name__ == '__main__':
    # 国美Excel列 写入数据
    GOME_2020_col = ['月份', '分部', '品牌', '日历天', '商品 - 键值', '商品 - 长文本', '23.销售数量', '3 销售净额（含促销补差）']
    GOME_2019_col = ['月份', '分部', '品牌', '日历天', '商品 - 键值', '商品 - 长文本', '23.销售数量', '销售净额']
    GOME_inventory_col=['分部','一二级分部','供应商','商品 - 长文本','商品 - 键值','库位标识（正品、残次、样机及赠品）','批次创建时间','采购类型','库存数量','含税库存金额']
    # 正式环境
    engine_read = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")
    # 导入数据
    # path_xlsx = r'E:\data\2020.xlsx'
    path_xlsx = r'E:\Share\每日导数\国美库存1123.xlsx'

    retail2db(path_xlsx, tablename='国美零售明细', sheetname='7-11', type='append', cols=GOME_2020_col,rownum=0)
    inventory2db(path_xlsx, tablename='国美库存明细', sheetname='整体', type='replace', cols=GOME_inventory_col,rownum=0)

    # 定义数据库连接
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/dwd")
    # 设定文件路径
    path_weishen_csv = r'\\10.157.2.94\共享文件\Data\GomeData\未审.csv'
    path_weishen_xlsx = r'\\10.157.2.94\共享文件\Data\GomeData\未审.xlsx'
    path_kaiti_xlsx = r"\\10.157.2.94\共享文件\Data\GomeData\开提.xls"
    path_gome_customer = r"\\10.157.2.94\共享文件\Data\GomeData\国美客户配置表.xlsx"
    path_gome_unplanned = r"\\10.157.2.94\共享文件\Data\GomeData\国美规划外型谱.xlsx"
    # 定义未审，开提字段
    weishen_cols = ["订单号", "单据状态", "销售公司名称", "经销商名称", "产品编码", "产品名称",
                    "订单未满足数量", "计划订单号", "计划订单周期", "计划订单状态", "划拨中心编码",
                    "划拨中心名称", "划拨客户编码", "划拨客户名称", "送货地址"]
    kaiti_cols = ["提货订单号", "产品编码", "产品名称", "经营主体", "营销大类名称", "营销小类名称",
                  "金额", "单位体积", "未发数量", "客户编码", "客户名称", "收货地址",
                  "划拨中心编码", "划拨中心名称", "划拨客户编码", "划拨客户名称"]
    # 定义库存sql,创建库存明细表
    sql_gome_inventory = """
    select d.对应中心 as 中心,a.一二级分部 as 国美一二级分部,a.分部 as 国美分部, a.批次创建时间 as 日期, a.供应商 ,
    b.奥维商品型号 as 商品型号,a.商品编码 ,a.库位标识 ,a.含税库存金额 ,a.库存数量 
    from ods.`国美库存明细` a 
    inner join dim.`国美奥维型号匹配表` b
    on a.商品型号 = b.国美商品型号
    inner join dim.奥维型号信息表 c
    on b.奥维商品型号 = c.奥维商品型号
    inner join (select distinct 国美分部 , 对应中心 from dim.国美分部中心配置 ) d
    on d.国美分部=a.分部  
    where a.库位标识 in ('残次','正常')
    """
    # 定义销售sql,创建近一个月销售表，按中心，机型，汇总
    sql_retail = """
    SELECT 中心, 品牌,奥维商品型号,sum(销量) as 销量, sum(销额) as 销额, 上市周度, 上市月度, 子品牌, 洗涤类型, 通用价格段新, 通用容量段新
    FROM dwd.国美零售明细 a
    where a.日期>=adddate(current_date(),interval -30 day)
    group by  中心, 品牌,奥维商品型号, 上市周度, 上市月度, 子品牌, 洗涤类型, 通用价格段新, 通用容量段新;
    """
    # 定义聚合字段
    group_array = ["中心", "产品名称"]
    # 调用函数完成csv转xlsx
    csv2xlsx(path_weishen_csv, path_weishen_xlsx, weishen_cols)
    # 中心配置表读入
    df_center = pd.read_sql("select * from dim.中心分部配置", con=engine)
    # 读取国美客户配置表
    df_gome_cus = pd.read_excel(path_gome_customer, sheet_name='GomeCustomer', header=0)
    # 未审数据读入
    df_weishen_temp = pd.read_excel(path_weishen_xlsx, header=0, usecols=weishen_cols, sheet_name='temp')
    # 重命名 未审数据的 【订单未满足数量】 为 【未发数量】
    df_weishen_temp.rename(columns={'订单未满足数量': '未发数量'}, inplace=True)
    # 未审数据处理，1.过滤订单号为空的未审数据 2.关联中心和国美客户
    df_weishen = df_weishen_temp.loc[df_weishen_temp.计划订单号.notnull()]. \
        merge(df_center, how='inner', left_on="划拨中心名称", right_on="分部名称"). \
        merge(df_gome_cus, how='inner', left_on="划拨客户名称", right_on="客户名称")
    # 未审产品机型分列 东芝机型修改replace
    df_weishen["产品名称"] = df_weishen["产品名称"].map(lambda x: x.split(" ", 1)[0].
                                                replace("858D3A13", "DGH-117X6D").
                                                replace("858D4A17", " DGH-117X6DZ"))
    # 因为未发数量是负整数，所以給绝对值
    df_weishen["未发数量"] = df_weishen["未发数量"].map(lambda x: abs(x))
    # 新增状态字段标记未审
    df_weishen["状态"] = "未审"
    # 开提数据导入
    df_kaiti_temp = pd.read_excel(path_kaiti_xlsx, header=0, usecols=kaiti_cols, sheet_name='sheet1')
    # 开提数据关联中心和国美客户
    df_kaiti = df_kaiti_temp.merge(df_center, how='inner', left_on="划拨中心名称", right_on="分部名称"). \
        merge(df_gome_cus, how='inner', left_on="划拨客户名称", right_on="客户名称")
    # 未审产品机型分列 东芝机型修改replace
    df_kaiti["产品名称"] = df_kaiti["产品名称"].map(lambda x: x.split(" ", 1)[0].
                                            replace("858D3A13", "DGH-117X6D").
                                            replace("858D4A17", " DGH-117X6DZ"))
    # 新增状态字段标记开提
    df_kaiti["状态"] = "开提"
    # 读入国美型谱
    df_type_list = pd.read_excel(path_gome_unplanned, header=0, sheet_name="Sheet1")
    # 读入库存明细
    df_gome_inventory = pd.read_sql(sql_gome_inventory, engine)
    # 制作库龄字段
    df_gome_inventory['库龄'] = (datetime.now() - df_gome_inventory['日期']).map(lambda x: x.days)
    # 按照45-90区分库龄
    df_gome_inventory['库龄状态'] = df_gome_inventory['库龄']. \
        map(lambda x: '未超45天' if x < 45 else ('超45天未超90天' if x < 90 else '90天及以上'))
    # 国美库存结果表：库存明细关联型谱 取规划内外字段 未关联上的默认在规划外
    df_gome_inventory_result = df_gome_inventory.merge(df_type_list, how='left', left_on='商品编码', right_on='商品 - 键值')[
        ["中心", "日期", "商品型号", "含税库存金额", "库存数量", "是否规划外", "库龄", "库龄状态"]].fillna(1)
    # 重命名 国美库存结果表的 【商品型号】 为 【奥维商品型号】
    df_gome_inventory_result = df_gome_inventory_result.rename(columns={"商品型号": "奥维商品型号"})
    # 库存按是否规划外透视
    result1 = df_gome_inventory_result.pivot_table(values=["含税库存金额", "库存数量"], index=["中心", "奥维商品型号"], columns="是否规划外",
                                                   aggfunc='sum', fill_value=0)
    # 库存按库龄状态透视
    result2 = df_gome_inventory_result.pivot_table(values=["含税库存金额", "库存数量"], index=["中心", "奥维商品型号"], columns="库龄状态",
                                                   aggfunc='sum', fill_value=0)
    # 整合开提未审数据 将订单数据生成 并重命名字段
    df_orders_result = df_kaiti[["中心", "未发数量", "产品名称", "状态"]].append(df_weishen[["中心", "未发数量", "产品名称", "状态"]]).fillna(0)
    df_orders_result = df_orders_result.rename(columns={"产品名称": "奥维商品型号", "状态": "进货状态", "未发数量": "进货数量"})
    # 订单数据透视 通过进货状态 也就是开提和未审的订单数量
    result3 = df_orders_result.pivot_table(values="进货数量", index=["中心", "奥维商品型号"], columns="进货状态", aggfunc='sum',
                                           fill_value=0)
    # 从数据库中读取国美零售数据
    df_gome_retail = pd.read_sql(sql_retail, engine)
    # 国美零售数据透视按中心和商品型号
    result4 = df_gome_retail.pivot_table(["销量", "销额"], index=["中心", "奥维商品型号"], aggfunc='sum', fill_value=0)
    # 关联进销存
    all_result = result1.merge(result2, how='inner', left_index=True, right_index=True) \
        .merge(result3, how='left', left_index=True, right_index=True) \
        .merge(result4, how='left', left_index=True, right_index=True).fillna(0)
    all_result.reset_index(inplace=True)
    all_result.columns = ['中心', '奥维商品型号', '规划内库存金额', '规划外库存金额', '规划内库存数量', '规划外库存数量', '超90天库存金额', '未超45天库存金额',
                          '超45天未超90天库存金额', '超90天库存数量', '未超45天库存数量', '超45天未超90天库存数量', '开提数量', '未审数量', '近30天销量', '近30天销额']
    # 读取奥维配置表和mmp出样数据
    sql_avc_type = "SELECT  品牌, 奥维商品型号, 上市月度, 上市周度, 洗涤类型, 子品牌, 通用价格段新, 通用容量段新 FROM dim.奥维型号信息表;"
    df_avc_type = pd.read_sql(sql_avc_type, engine)
    sample_sql = 'select 分部, 型号 from ods.样机上样数据 where 门店一级分类 = "国美";'
    center_sql = 'select * from dim.中心分部配置'
    gome_avc_sql = 'select * from dim.国美奥维型号匹配表'
    sample_df = pd.read_sql(sample_sql, con=engine)
    center_df = pd.read_sql(center_sql, con=engine)
    gome_avc_df = pd.read_sql(gome_avc_sql, con=engine)
    # 处理出样数据型号
    sample_df['型号'] = sample_df['型号'].map(lambda x: x.split(' ')[0]).replace('858D4A17', 'DGH-117X6DZ').replace(
        '858D3A13', 'DGH-117X6D')
    # 出样数据关联中心配置表 取中心后关联国美奥维型号
    sample_df = sample_df.merge(center_df[['分部名称', '中心']].rename(columns={'分部名称': '分部'}))
    sample_df = sample_df.merge(gome_avc_df.rename(columns={'奥维商品型号': '型号'}), how='left')
    # 关联出样和奥维信息表
    result5 = sample_df.drop(columns=['分部']).groupby(by=['中心', '型号']).count().reset_index().rename(
        columns={'国美商品型号': '出样数量'})
    all_result = all_result.merge(df_avc_type, how='left', on='奥维商品型号').merge(result5.rename(columns={'型号': '奥维商品型号'}),
                                                                     how='left').fillna(0)
    all_result.to_sql("国美进销存", engine, index=False, if_exists='replace')


