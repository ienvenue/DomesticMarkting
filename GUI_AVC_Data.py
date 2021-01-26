from tkinter import ttk,filedialog,messagebox
import tkinter as tk
import pandas as pd
from sqlalchemy import create_engine
import logging
import pymysql

# 定义数据库
engine = create_engine("mysql+pymysql://data_dev:data_dev0.@10.157.2.94:3306/aowei")
# 定义字段
月度线上干衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式',
                      '品牌类型', '容量', '上市月度', '上市周度', '子品牌', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段']

月度线上洗衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸', '电机类型',
                       '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度', '洗涤类型', '智能',
                       '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段'
                       ]
周度线上洗衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型三星', '产品系列', '尺寸',
                      '电机类型', '耗电量', '烘干', '烘干类型', '厚度', '控制方式', '能效等级', '品牌类型', '容量', '上市月度', '上市周度',
                      '洗涤类型', '智能', '转速', '子品牌', '价格段', '容量段', '洗涤价格段1', '洗涤容量段', '小天鹅价格段', '小天鹅容量段']

周度线上干衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '电商1', '变频', '产品类型', '干衣方式', '控制方式', '品牌类型', '容量',
                     '上市月度', '上市周度', '通用价格段', '线上价格段', '小天鹅价格段', '小天鹅容量段', '子品牌'
                     ]
月度线下干衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '干衣方式', '上市月度', '上市周度', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                       '线下价格段', '线下容量段']

月度线下洗衣机 = ['销量', '销额', '单价', '月度', '品牌', '机型', '上市月度', '上市周度', '洗涤类型', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                        '通用价格段新', '通用容量段新']

周度线下洗衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '品类', '变频', '产品类型', '产品系列', '尺寸',
                       '电机类型', '耗电量', '烘干', '烘干类型', '厚度', '能效等级', '品牌类型', '容量', '上市月度', '上市周度',
                       '洗涤类型', '智能', '转速', '子品牌', '通用价格段新', '通用容量段新', '洗涤价格段改', '洗涤容量段改', '小天鹅价格段', '小天鹅容量段',
                       '大区', '美的渠道', '省份', '洗衣机美的片区', '洗衣机美的中心', '洗衣机美的中心2','地市'
                       ]
周度线下干衣机 = ['销量', '销额', '单价', '周度', '品牌', '机型', '干衣方式', '上市月度', '上市周度', '子品牌', '美的渠道', '省份', '洗衣机美的中心',
                      '线下价格段', '线下容量段']

# 第1步，实例化object，建立窗口window
window=tk.Tk()

# 第2步，给窗口的可视化起名字
window.title('AVC数据导入')

# 第3步，设定窗口的大小(长 * 宽)
window.geometry('400x300+400+300')  # 这里的乘是小x

# 第4步，定义label
tk.Label(window, text='文件路径:', font=('微软雅黑', 12)).place(x=35, y=30)
tk.Label(window, text='导入表名:', font=('微软雅黑', 12)).place(x=35, y=70)
tk.Label(window, text='Sheet名:', font=('微软雅黑', 12)).place(x=35, y=110)
tk.Label(window, text='导入方式:', font=('微软雅黑', 12)).place(x=35, y=150)
tk.Label(window, text='文件类型:', font=('微软雅黑', 12)).place(x=35, y=190)

# 第5步，定义输入框entry和选择框
# 地址
var_path=tk.StringVar()
entry_path=tk.Entry(window, textvariable=var_path, font=('微软雅黑', 12))
entry_path.place(x=120, y=30)
# cols
var_cols=tk.StringVar(value='月度线上洗衣机')
entry_cols=ttk.Combobox(window, textvariable=var_cols, width=18, font=('微软雅黑', 12))
entry_cols.place(x=120, y=70)
entry_cols['values']=('周度线上洗衣机', '周度线下洗衣机', '周度线上干衣机', '周度线下干衣机','月度线上洗衣机', '月度线下洗衣机', '月度线上干衣机', '月度线下干衣机' )
# sheet页名
var_sheetname=tk.StringVar(value='Sheet2')
entry_sheetname=tk.Entry(window, textvariable=var_sheetname, font=('微软雅黑', 12))
entry_sheetname.place(x=120, y=110)
# type
var_type_name=tk.StringVar(value='追加')
entry_type_name=ttk.Combobox(window, textvariable=var_type_name, width=18, font=('微软雅黑', 12))
entry_type_name.place(x=120, y=150)
entry_type_name['values']=('覆盖', '追加')
# row
file_type=tk.StringVar(value='xlsx')
entry_file_type=ttk.Combobox(window, textvariable=file_type, width=18, font=('微软雅黑', 12))
entry_file_type.place(x=120, y=190)
entry_file_type['values']=('xlsx', 'csv')

def avc_month_xlsx2db(file_path, table_name, sheet_name, type_name, cols, header):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=cols)
        df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)
    except Exception as e:
        logging.exception(e)

def avc_week_xlsx2db(file_path, table_name, sheet_name, type_name, cols, header):
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, usecols=cols)
        df['年'] = df['周度'].map(lambda x: x.split('W')[0])
        df['周'] = df['周度'].map(lambda x: x.split('W')[1])
        df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)
    except Exception as e:
        logging.exception(e)

def avc_month_csv2db(file_path, table_name, type_name, cols, header):
    try:
        df = pd.read_csv(file_path, header=header, usecols=cols, encoding="gbk")
        df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)
    except Exception as e:
        logging.exception(e)

def avc_week_csv2db(file_path, table_name, type_name, cols, header):
    try:
        df = pd.read_csv(file_path, header=header, usecols=cols, encoding="gbk", engine='python')
        df['年'] = df['周度'].map(lambda x: x.split('W')[0])
        df['周'] = df['周度'].map(lambda x: x.split('W')[1])
        df.to_sql(name=table_name, con=engine, if_exists=type_name, index=False)
    except Exception as e:
        logging.exception(e)

# 绝对路径替换
def selectPath():
    path_=filedialog.askopenfilename()
    path_=path_.replace("/", "\\\\")
    var_path.set(path_)


# 第6步，导入数据
def import_data():
    # 获取用户输入
    path=var_path.get()
    tablename=var_cols.get()
    sheetname=var_sheetname.get()
    type_name=var_type_name.get().replace('覆盖', 'replace').replace('追加', 'append')
    cols=eval(var_cols.get())
    try:
        if tablename[0:2] == '周度':
            avc_week_xlsx2db(path,tablename,sheetname,type_name,cols,0)
            messagebox.showinfo(tablename+'提示', '导入成功')
            window.mainloop()
        elif tablename[0:2] == '月度':
            avc_month_xlsx2db(path, tablename, sheetname, type_name, cols, 0)
            messagebox.showinfo(tablename+'提示', '导入成功')
            window.mainloop()
        else:
            messagebox.showerror('提示', '导入失败请确认')
    except:
        messagebox.showerror('提示', '导入失败')

def check_data():
    path = var_path.get()
    tablename = var_cols.get()
    sheetname = var_sheetname.get()
    type_name = var_type_name.get()
    messagebox.showinfo('导入数据核对', '数据库表名：'+tablename+'\n'+''+'文件路径：'+path+'\n'+'Sheet名：'+sheetname+'\n'+'导入方式：'+type_name+'\n')


# 第7步，导入按钮和选择路径按钮
btn_check=tk.Button(window, text='确认信息', command=check_data, font=('微软雅黑', 10)).place(x=120, y=230)
btn_import=tk.Button(window, text='确认导入', command=import_data, font=('微软雅黑', 10)).place(x=230, y=230)
btn_path=tk.Button(window, text="路径选择", command=selectPath, font=('微软雅黑', 10)).place(x=320, y=25)

# 第8步，主窗口循环显示
window.mainloop()
