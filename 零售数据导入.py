import logging
import tkinter as tk
from tkinter import *
import time
from tkinter import filedialog, messagebox,ttk
from sqlalchemy import create_engine
import update_all_data as ud
import import_retail_data as ip

bx_mmp_use_cols = ['上报ID', '上报单号', '来源单号', '上报类型', '产品类型', '商品型号', '商品编码', '商品名称', '大类', '小类', '商品备注', '门店编码', '门店名称',
                   '门店等级', '省份(门店)','地级市(门店)', '县/区(门店)', '经营单位编码', '经营单位名称', '所属代理商编码', '所属代理商名称', '导购经销商编码', '导购经销商名称', '门店一级分类', '门店二级分类', '门店三级分类',
                   '事业部分类', '导购员编码', '导购员名称', '导购员手机号', '导购类型', '数量', '单价', '零售价', '总价', '资源抵扣金额', '分部编码', '分部名称',
                   'CMDM中心编码', '大区', '容量', '容量段', '品类', '产品线', '顾客固话', '是否手工导入', '顾客手机', '上报时间',
                   '所属区域', '来源系统',  '创建时间']

channel_use_col = ['中心编码', '中心名称', '卖方商务中心编码', '卖方商务中心',
                   '卖方合作模式大类(CRM)/一级分类(CMDM)', '卖方合作模式小类(CRM)/二级分类(CMDM)',
                   '卖方客户编码', '卖方客户名称', '是否有效客户', '渠道层级', '单据日期', '出库确认日期',
                   '买方客户编码', '买方客户名称', '买方合作模式大类(CRM)/一级分类(CMDM)',
                   '品类', '产品线', '仓库编码', '仓库名称', '营销小类', '商品编码', '商品名称',
                   '销售型号', '门店编码', '门店名称', '门店一级分类', '门店二级分类', '开单数量',
                   '出库确认数量', '含税价(折后)', '开单金额(折前)', '签收时间']

mmp_use_col = ['订单分类','套餐编码','套餐名称','套餐数量','套餐单价','套餐抵扣金额','上报ID', '上报单号', '来源单号', '订单来源', '上报类型',
                '产品类型', '商品型号', '商品编码', '商品名称', '大类', '小类', '商品备注',
            '智能属性', '门店编码', '门店名称', '门店等级', '经营单位编码', '经营单位名称', '所属代理商编码', '所属代理商名称', '导购经销商编码',
            '导购经销商名称', '门店一级分类', '门店二级分类', '门店三级分类', '事业部分类', '导购员编码', '导购员名称', '导购员手机号', '导购类型',
            '数量', '单价', '零售价', '总价', '资源抵扣金额', '厂家承担券', '系统承担券', '券后金额',  '分部编码', '分部名称',
            'CMDM中心编码', '大区', '省份(门店)', '地级市(门店)', '县/区(门店)', '品牌',  '容量', '容量段',
            '产品定位', '品类', '产品线', '顾客固话', '是否手工导入', '顾客手机',
            '上报时间', '备注', '配件', '条码', '条码编码', '条码型号', '条码名称',
            '条码备注', '扫码类型', '与CSS是否重复', '与CCS是否重复', '是否补贴依据', '是否计算补贴', '返现金额', '带单金额',  '带单电话', '卡号', '所属区域',
            '验证码', 'OMS码', '代收货款', '配送时间', '安装时间', '配送方式', '来源系统', '是否有导购', '零售主管', '认筹券号', '美的通版本', '创建人', '创建时间',
            '是否美居用户', '激活属性', '激活补录手机号', '激活时间']

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
entry_cols['values']=('洗衣机MMP', '一级渠道', '二级渠道','冰箱MMP')
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


# 第6步，导入数据
def import_data():
    # 获取用户输入
    path=var_path.get()
    tablename=var_cols.get().replace('冰箱MMP', 'mmp冰箱').replace('洗衣机MMP', 'mmp零售数据全量').replace('一级渠道', '一级代理渠道零售数据').replace('二级渠道', '二级代理渠道零售数据')
    sheetname='Sheet0' if tablename in ('mmp零售数据全量','mmp冰箱') else '   渠道出库明细'
    type_name='append'
    cols=var_cols.get().replace('冰箱MMP', 'bx_mmp_use_cols').replace('洗衣机MMP', 'mmp_use_col').replace('一级渠道', 'channel_use_col').replace('二级渠道', 'channel_use_col')
    row=0 if tablename in ('mmp零售数据全量','mmp冰箱')  else 1
    try:
        df=ip.load_data(path,sheetname,row,eval(cols))
        ip.del_data(df,tablename)
        ip.save_data(df,tablename,type_name)
        write_log_to_text(tablename+'正在导入，请等待'+ "\n")
        messagebox.showinfo('提示', '导入成功'+ "\n")
        write_log_to_text(tablename+'导入成功!'+ "\n")
    except Exception as e:
        logging.exception(e)
        messagebox.showerror('提示', '导入失败'+ "\n")
        write_log_to_text(tablename + '导入失败，出现如下异常：%s'%e+"\n")
    window.mainloop()

# 第7步，导入按钮和选择路径按钮
btn_import=tk.Button(window, text='确认导入', command=import_data, font=('微软雅黑', 10)).place(x=120, y=230)
btn_path=tk.Button(window, text="路径选择", command=selectPath, font=('微软雅黑', 10)).place(x=320, y=25)
btn_export=tk.Button(window, text="更新数据", command=ud.update_offline_data, font=('微软雅黑', 10)).place(x=220, y=230)

# 第8步，主窗口循环显示
window.mainloop()
