import tkinter as tk
from tkinter import *
import time
from tkinter import filedialog, messagebox,ttk
from sqlalchemy import create_engine
import update_all_data as ud
import import_retail_data as ip

channel_use_col = ['中心编码', '中心名称', '卖方商务中心编码', '卖方商务中心',
                   '卖方合作模式大类(CRM)/一级分类(CMDM)', '卖方合作模式小类(CRM)/二级分类(CMDM)',
                   '卖方客户编码', '卖方客户名称', '是否有效客户', '渠道层级', '单据日期', '出库确认日期',
                   '买方客户编码', '买方客户名称', '买方合作模式大类(CRM)/一级分类(CMDM)',
                   '品类', '产品线', '仓库编码', '仓库名称', '营销小类', '商品编码', '商品名称',
                   '销售型号', '门店编码', '门店名称', '门店一级分类', '门店二级分类', '开单数量',
                   '出库确认数量', '含税价(折后)', '开单金额(折前)', '签收时间']

mmp_use_col = ['上报ID','商品型号','商品编码','大类','小类','门店编码','门店名称','门店等级',
              '门店一级分类','门店二级分类','导购员编码','导购员名称','导购员手机号',
              '导购类型','数量','单价','零售价','总价','资源抵扣金额','厂家承担券',
              '分部名称','CMDM中心编码','产品线','创建时间','顾客手机','上报单号','来源单号','上报类型','订单分类','套餐编码','套餐名称','套餐数量','套餐单价','套餐抵扣金额']

LOG_LINE_NUM = 0

# 数据库正式环境
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/ods")

# 第1步，实例化object，建立窗口window
window=tk.Tk()

# 第2步，给窗口的可视化起名字
window.title('数据导入工具')

# 第3步，设定窗口的大小(长 * 宽)
window.geometry('400x300+400+300')  # 这里的乘是小x

# 第4步，定义label
tk.Label(window, text='文件路径:', font=('微软雅黑', 12)).place(x=35, y=30)
tk.Label(window, text='导入表名:', font=('微软雅黑', 12)).place(x=35, y=70)
# tk.Label(window, text='完成进度:', font=('微软雅黑', 12)).place(x=35, y=110)
tk.Label(window, text='程序日志:', font=('微软雅黑', 12)).place(x=35, y=110)




# 第5步，定义输入框entry和选择框
# 地址
var_path=tk.StringVar()
entry_path=tk.Entry(window, textvariable=var_path, font=('微软雅黑', 12))
entry_path.place(x=120, y=30)
# cols
var_cols=tk.StringVar()
entry_cols=tk.ttk.Combobox(window, textvariable=var_cols, width=18, font=('微软雅黑', 12))
entry_cols.place(x=120, y=70)
entry_cols['values']=('MMP零售', '一级渠道', '二级渠道')
# canvas = tk.Canvas(window, width=185, height=24, bg="white").place(x=120, y=110)
log_data_Text=tk.Text(window, width=28, height=8)
log_data_Text.place(x=120, y=110)  # 日志框

# 获取当前时间
def get_current_time():
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return current_time

# 日志打印
def write_log_to_text(logmsg):
    global LOG_LINE_NUM
    current_time = get_current_time()
    logmsg_in = str(current_time) + " " + str(logmsg) + "\n"  # 换行
    if LOG_LINE_NUM <= 7:
        log_data_Text.insert(END, logmsg_in)
        LOG_LINE_NUM = LOG_LINE_NUM + 1
    else:
        log_data_Text.delete(1.0, 2.0)
        log_data_Text.insert(END, logmsg_in)


# 绝对路径替换
def selectPath():
    path_=filedialog.askopenfilename()
    path_=path_.replace("/", "\\\\")
    var_path.set(path_)

# 显示下载进度
# def progress():
#     # 填充进度条
#     fill_line = canvas.create_rectangle(1.5, 1.5, 0, 23, width=0, fill="green")
#     x = 500  # 未知变量，可更改
#     n = 185 / x  # 185是矩形填充满的次数
#     for i in range(x):
#         n = n + 185 / x
#         canvas.coords(fill_line, (0, 0, n, 60))
#         window.update()
#         time.sleep(0.01)  # 控制进度条流动的速度
#
#     # 清空进度条
#     fill_line = canvas.create_rectangle(1.5, 1.5, 0, 23, width=0, fill="white")
#     x = 500  # 未知变量，可更改
#     n = 185 / x  # 185是矩形填充满的次数
#
#     for t in range(x):
#         n = n + 185 / x
#         # 以矩形的长度作为变量值更新
#         canvas.coords(fill_line, (0, 0, n, 60))
#         window.update()
#         time.sleep(0)

# 第6步，导入数据
def import_data():
    # 获取用户输入
    path=var_path.get()
    tablename=var_cols.get().replace('MMP零售', 'mmp零售数据全量').replace('一级渠道', '一级代理渠道零售数据').replace('二级渠道', '二级代理渠道零售数据')
    sheetname='Sheet0' if tablename=='mmp零售数据全量' else '   渠道出库明细'
    type_name='append'
    cols=var_cols.get().replace('MMP零售', 'mmp_use_col').replace('一级渠道', 'channel_use_col').replace('二级渠道', 'channel_use_col')
    row=0 if tablename=='mmp零售数据全量' else 1
    try:
        df=ip.load_data(path,sheetname,row,eval(cols))
        ip.del_data(df,tablename)
        ip.save_data(df,tablename,type_name)
        write_log_to_text(tablename+'正在导入，请等待'+ "\n")
        messagebox.showinfo('提示', '导入成功'+ "\n")
        write_log_to_text(tablename+'导入成功!'+ "\n")
    except:
        messagebox.showerror('提示', '导入失败'+ "\n")
        write_log_to_text(tablename + '导入失败,请确认表名和文件名'+ "\n"  )
    window.mainloop()

# 第7步，导入按钮和选择路径按钮
btn_import=tk.Button(window, text='确认导入', command=import_data, font=('微软雅黑', 10)).place(x=120, y=230)
btn_path=tk.Button(window, text="路径选择", command=selectPath, font=('微软雅黑', 10)).place(x=320, y=25)
btn_export=tk.Button(window, text="更新数据", command=ud.update_offline_data, font=('微软雅黑', 10)).place(x=220, y=230)

# 第8步，主窗口循环显示
window.mainloop()
