from win32com.client import Dispatch
import pandas as pd
from sqlalchemy import create_engine

def csv2xlsx(path_weishen_csv,path_weishen_xlsx,cols):
    csv_temp_df = pd.read_csv(path_weishen_csv, header=1, usecols=cols, encoding="gbk")
    csv_temp_df.to_excel(path_weishen_xlsx, sheet_name='temp')
    app = Dispatch("Excel.Application")
    app.Visible = False
    content = app.Workbooks.Open(path_weishen_xlsx)
    content.Save()
    content.Close()

if __name__ == '__main__':
    # 定义数据库连接
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")
    # 设定文件路径
    path_weishen_csv = r'D:\Code\jupyter\xlsx\未审.csv'
    path_weishen_xlsx = r'D:\Code\jupyter\xlsx\未审.xlsx'
    path_kaiti_xlsx = r"D:\Code\jupyter\xlsx\开提.xls"
    path_gome_customer = r"D:\Code\jupyter\xlsx\国美客户配置表.xlsx"
    # 定义字段
    weishen_cols = ["订单号", "单据状态", "销售公司名称", "经销商名称", "产品编码", "产品名称",
                    "订单未满足数量", "计划订单号", "计划订单周期", "计划订单状态", "划拨中心编码",
                    "划拨中心名称", "划拨客户编码", "划拨客户名称", "送货地址"]
    kaiti_cols = ["提货订单号", "产品编码", "产品名称", "经营主体", "营销大类名称", "营销小类名称",
                  "金额", "单位体积", "未发数量", "客户编码", "客户名称", "收货地址",
                  "划拨中心编码", "划拨中心名称", "划拨客户编码", "划拨客户名称"]
    # 定义聚合字段
    group_array = ["中心", "产品名称"]
    # 调用函数完成csv转xlsx
    csv2xlsx(path_weishen_csv, path_weishen_xlsx, weishen_cols)
    # 中心配置表读入
    df_center = pd.read_sql("select * from dim.中心分部配置", con=engine)
    # 读取国美客户配置表
    df_gome_cus = pd.read_excel(path_gome_customer,sheet_name='GomeCustomer',header=0)
    # 未审数据读入
    df_weishen_temp = pd.read_excel(path_weishen_xlsx, header=0, usecols=weishen_cols, sheet_name='temp')
    df_weishen_temp.rename(columns={'订单未满足数量': '未发数量'}, inplace=True)
    # 未审数据关联中心和国美客户
    df_weishen = df_weishen_temp.loc[df_weishen_temp.计划订单号.notnull()].\
        merge(df_center, how='inner', left_on="划拨中心名称", right_on="分部名称").\
        merge(df_gome_cus,how='inner',left_on="划拨客户名称",right_on="客户名称")
    df_weishen["产品名称"] = df_weishen["产品名称"].map(lambda x: x.split(" ", 1)[0].
                                                replace("858D3A13", "DGH-117X6D").
                                                replace("858D4A17", " DGH-117X6DZ"))
    df_weishen["未发数量"] = df_weishen["未发数量"].map(lambda x: abs(x))
    # 开提数据导入和关联中心和国美客户
    df_kaiti_temp = pd.read_excel(path_kaiti_xlsx, header=0, usecols=kaiti_cols, sheet_name='sheet1')
    df_kaiti = df_kaiti_temp.merge(df_center, how='inner', left_on="划拨中心名称", right_on="分部名称").\
        merge(df_gome_cus,how='inner',left_on="划拨客户名称",right_on="客户名称")
    df_kaiti["产品名称"] = df_kaiti["产品名称"].map(lambda x: x.split(" ", 1)[0].
                                            replace("858D3A13", "DGH-117X6D").
                                            replace("858D4A17", " DGH-117X6DZ"))
    # 整合未审和开提，生成订单明细表写入数据库
    result = df_kaiti[["中心", "未发数量", "产品名称"]].append(df_weishen[["中心", "未发数量", "产品名称"]]).fillna(0)
    result.to_sql('国美订单明细', engine, if_exists='replace', index_label='自增主键')