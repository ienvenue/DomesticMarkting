import tkinter as tk  # 使用Tkinter前需要先导入
import tkinter.messagebox
import time
import pandas as pd
from sqlalchemy import create_engine
from tkinter import ttk
from tkinter.filedialog import askdirectory



mmp_use_col = ['商品型号', '商品编码', '大类', '小类', '门店编码', '门店名称', '门店等级',
               '门店一级分类', '门店二级分类', '导购员编码', '导购员名称', '导购员手机号',
               '导购类型', '数量', '单价', '零售价', '总价', '资源抵扣金额',
               '分部名称', 'CMDM中心编码', '产品线', '创建时间']

channel_use_col = ['中心编码', '中心名称', '卖方商务中心编码', '卖方商务中心',
                   '卖方合作模式大类(CRM)/一级分类(CMDM)', '卖方合作模式小类(CRM)/二级分类(CMDM)',
                   '卖方客户编码', '卖方客户名称', '是否有效客户', '渠道层级', '单据日期', '出库确认日期',
                   '买方客户编码', '买方客户名称', '买方合作模式大类(CRM)/一级分类(CMDM)',
                   '品类', '产品线', '仓库编码', '仓库名称', '营销小类', '商品编码', '商品名称',
                   '销售型号', '门店编码', '门店名称', '门店一级分类', '门店二级分类', '开单数量',
                   '出库确认数量', '含税价(折后)', '开单金额(折前)', '签收时间']

sample_use_col = ['导购编码', '导购员姓名', '导购员手机', '分部', '门店编码',
                  '门店名称', '门店一级分类', '门店二级分类', '门店等级', '代理商编码',
                  '代理商名称', '上样时间', '商品大类', '主体', '型号编码', '产品线',
                  '型号', '智能属性', '智能体验情况', '智能体验设备', '样机条码',
                  '类型', '样机分类', '是否竞品', '门体数量', '品牌']

store_use_col = ['门店名称', '门店编码', '千店导购', '虚拟门店', '一级分类', '二级分类', '经营状态',
                 '所属事业部', '事业部编码', '所属分部', '中心编码', '经营单位类型', '经营单位编码',
                 '经营单位名称', '事业部分类', '分销商供货方名称', '所属代理商编码', '所属代理商名称',
                 '产品线名称', '省', '市', '县/区', '镇', '镇/街道编码', '详细地址', '集团运营中心',
                 '客户门店编码', '客户门店名称', '网点分级', '门店等级', '市场等级', '创建时间',
                 '更新时间', 'TERMINAL_ORG_ID', 'TERMINAL_ID']

# 正式环境
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

# 第1步，实例化object，建立窗口window
window=tk.Tk()

# 第2步，给窗口的可视化起名字
window.title('数据导入工具')

# 第3步，设定窗口的大小(长 * 宽)
window.geometry('400x300')  # 这里的乘是小x

# 第4步，定义label
tk.Label(window, text='文件路径:', font=('微软雅黑', 12)).place(x=35, y=30)
tk.Label(window, text='导入系统:', font=('微软雅黑', 12)).place(x=35, y=70)
tk.Label(window, text='Sheet页名:', font=('微软雅黑', 12)).place(x=35, y=110)
tk.Label(window, text='导入方式:', font=('微软雅黑', 12)).place(x=35, y=150)
tk.Label(window, text='从N行开始:', font=('微软雅黑', 12)).place(x=35, y=190)

# 第5步，定义输入框entry和选择框
# 地址
var_path=tk.StringVar()
entry_path=tk.Entry(window, textvariable=var_path, font=('微软雅黑', 12))
entry_path.place(x=120, y=30)
# cols
var_cols=tk.StringVar()
entry_cols=ttk.Combobox(window, textvariable=var_cols, width=18, font=('微软雅黑', 12))
entry_cols.place(x=120, y=70)
entry_cols['values']=('MMP零售', '一级渠道', '二级渠道', '出样上样', '终端门店')
# sheet页名
var_sheetname=tk.StringVar()
entry_sheetname=tk.Entry(window, textvariable=var_sheetname, font=('微软雅黑', 12))
entry_sheetname.place(x=120, y=110)
# type
var_type_name=tk.StringVar()
entry_type_name=ttk.Combobox(window, textvariable=var_type_name, width=18, font=('微软雅黑', 12))
entry_type_name.place(x=120, y=150)
entry_type_name['values']=('覆盖', '追加')
# row
var_row=tk.StringVar()
entry_row=tk.Entry(window, textvariable=var_row, font=('微软雅黑', 12))
entry_row.place(x=120, y=190)

# 绝对路径替换
def selectPath():
    path_=tkinter.filedialog.askopenfilename()
    path_=path_.replace("/", "\\\\")
    var_path.set(path_)


# 第6步，导入数据
def import_data():
    # 获取用户输入
    path=var_path.get()
    tablename=var_cols.get().replace('MMP零售', 'mmp零售数据').replace('一级渠道', '一级代理渠道零售数据')\
        .replace('二级渠道', '二级代理渠道零售数据').replace('出样上样', '样机上样数据').replace('终端门店', '终端门店数据')
    sheetname=var_sheetname.get()
    type_name=var_type_name.get().replace('覆盖', 'replace').replace('追加', 'append')
    cols=var_cols.get().replace('MMP零售', 'mmp_use_col').replace('一级渠道', 'channel_use_col') \
        .replace('二级渠道', 'channel_use_col').replace('出样上样', 'sample_use_col').replace('终端门店', 'store_use_col')
    row=int(var_row.get())
    try:
        print(tablename + "导入开始时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        df = pd.read_excel(path, sheet_name=sheetname, header=row, usecols=cols)
        df.to_sql(name=tablename, con=engine, if_exists=type_name, index=False)
        print(tablename + "导入结束时间 :", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        tkinter.messagebox.showinfo('提示', '导入成功')
        window.destroy()
    except:
        tkinter.messagebox.showerror('提示', '导入失败')


# 第7步，导入按钮和选择路径按钮
btn_import=tk.Button(window, text='确认导入', command=import_data, font=('微软雅黑', 10)).place(x=120, y=230)
btn_path=tk.Button(window, text="路径选择", command=selectPath, font=('微软雅黑', 10)).place(x=320, y=25)

# 第8步，主窗口循环显示
window.mainloop()
