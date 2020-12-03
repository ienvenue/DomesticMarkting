import pandas as pd
import os
import time

print('正在汇总考勤表...')

file_path = r'D:\考勤基表'

os.chdir(file_path)

file_lst = [file for file in os.listdir()]

df_offical = pd.DataFrame()
for file in file_lst:
    frame = pd.read_excel(file, header=4, sheet_name='正式体系')
    df_offical = df_offical.append(frame)

df_terminal = pd.DataFrame()
for file in file_lst:
    frame = pd.read_excel(file, header=4, sheet_name='终端体系')
    df_terminal = df_terminal.append(frame)

df_offical.to_excel(r'D:\考勤基表\正式体系汇总.xlsx', index=False)
df_terminal.to_excel(r'D:\考勤基表\终端体系汇总.xlsx', index=False)

print('考勤表汇总完毕，程序将在5秒后关闭')

time.sleep(5)