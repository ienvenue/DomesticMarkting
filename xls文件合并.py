import pandas as pd
import os
import time

print('正在汇总表...')

file_path = r'D:\汇总'
file=r'D:\汇总\汇总.xlsx'

if  os.path.exists(file):
    os.remove(file)

os.chdir(file_path)

file_lst = [file for file in os.listdir()]

df_offical = pd.DataFrame()
for file in file_lst:
    try:
        frame = pd.read_excel(file, header=0, sheet_name='Sheet1')
    except:
        print(file)
    df_offical = df_offical.append(frame)

df_offical.to_excel(r'D:\汇总\汇总.xlsx', index=False)


print('表汇总完毕，程序将在5秒后关闭')

time.sleep(5)