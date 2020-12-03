import time
from win32com.client import Dispatch
import pandas as pd
from sqlalchemy import create_engine

fahuo_cols = ['客户编码', '客户名称', '客户类型', '直发标志', '营销大类', '单据类型', '单据号', '销售区域', '营销中心', '营销中心名称', '账户', '单据日期', '录入日期',
              '发货日期', '结算日期', '对账日期', '审核标识', '结算标识', '签收日期', '签收标记', '发票日期', '发票号', '开票单位', '发货仓库编码', '发货仓库名称',
              '收货仓库编码', '收货仓库名称', '工程机标识', '事业部客户', 'ERP OU名称', '产品编码', '产品名称', '品牌', '营销小类', '产品形态', '定频/变频',
              '能效等级', '产品单位', '产品价格', '结算价格', '批文价格', '产品数量', '已红冲数量', '退货数量', '签收数量', '退货签收数量', '列表金额', '结算金额',
              '批文金额', '月返', '折扣率', '月返金额', '折扣金额', '已红冲金额', '产品单位重量', '产品单位体积', '批文单号', '批文类型', '来源类型', '来源单号',
              '调账原财务单号', '关联客户编码', '关联客户名称', '样机出样门店编码', '样机出样门店名称', '客户订单号', '客户订单日期', '发货通知单', '原单据号', '制单人',
              '收货地址', '头备注', '行备注', '上级来源单号', '上级来源类型', '最近更新人(头)', '最近更新时间(头)', '最近更新人(行)', '最近更新时间(行)', '主体名称',
              '发票类型', 'ECM物流单号', '税码', '中间费用（返利金额）', '中间折扣（返利率）', '开票状态', '排车编号', '运输合同号', '产品备注', '折扣类型', '结算类型名称',
              '签收系统', '产品竞争属性', '转采购标志', '转采购信息', '新中心编码', '新中心名称', '划拨中心编码', '划拨中心名称', '划拨客户编码', '划拨客户名称', '划拨比例',
              '中间费用（费用金额）', '中间折扣（费用率）', '物流处理类型', 'ERP应收发票号', '数据来源', '外部系统单据号', '支付方式', '合同差异开单', '支付来源',
              '经营渠道类型', 'PO接收标识', 'PO接收日期']

def excel2db(path, tablename, sheetname, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_excel(path, sheet_name=sheetname, header=rownum, usecols=cols)
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def csv2db(path, tablename, type, cols, rownum):
    print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    df = pd.read_csv(path, header=rownum, usecols=cols, encoding="gbk")
    df.to_sql(name=tablename, con=engine, if_exists=type, index=False)
    print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))


def just_open(path):
    """
    打开文件并保存推出
    """
    app = Dispatch("Excel.Application")
    app.Visible = False
    content = app.Workbooks.Open(path)
    content.Save()
    content.Close()


if __name__ == '__main__':
    # 正式环境
    engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

    # 测试环境
    # engine=create_engine("mysql+pymysql://test:test123@10.157.6.74:3306/ods")
    print("转化开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    path_csv = r'E:\Share\每日导数\11.16cims.csv'
    path_xlsx = r'E:\Share\每日导数\11.16cims.xlsx'
    csv_temp_df = pd.read_csv(path_csv, header=0, usecols=fahuo_cols, encoding="gbk")
    csv_temp_df.to_excel(path_xlsx, sheet_name='cims')
    print("转化结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    just_open(path_xlsx)
    excel2db(path_xlsx, tablename='发货数据明细', sheetname='cims', type='append', cols=fahuo_cols,
             rownum=0)
