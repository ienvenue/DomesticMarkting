import tkinter as tk  # 使用Tkinter前需要先导入
import tkinter.messagebox
import pickle
import sample_store as ss
from tkinter.filedialog import askdirectory, askopenfilename
from tkinter import ttk

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
        ss.excel2db(path, tablename, sheetname, type_name, cols, row)
        tkinter.messagebox.showinfo('提示', '导入成功')
        window.destroy()
    except:
        tkinter.messagebox.showerror('提示', '导入失败')


# 第7步，导入按钮和选择路径按钮
btn_import=tk.Button(window, text='确认导入', command=import_data, font=('微软雅黑', 10)).place(x=120, y=230)
btn_path=tk.Button(window, text="路径选择", command=selectPath, font=('微软雅黑', 10)).place(x=320, y=25)

# 第8步，主窗口循环显示
window.mainloop()
