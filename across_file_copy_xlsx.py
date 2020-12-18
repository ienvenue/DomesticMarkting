import win32com.client as win32
import os

def get_dir(file_path):
	file_list =  os.listdir(file_path)
	return file_list

def across_file_copy(file_path,file_list):
	exc_tool = win32.Dispatch('Excel.Application')
	# 设置打开的Excel表格为可见状态；忽略则Excel表格默认不可见
	exc_tool.visible = 1
	# 新建立Excel文件
	target_file = exc_tool.Workbooks.Add()
	target_table_list = target_file.Worksheets
	for file_name in file_list:
		file_name_dec = file_name.split('.')
		print(file_name_dec)
		if file_name_dec[-1] == 'xlsx' or file_name_dec[-1] == 'xls':
			file = exc_tool.Workbooks.Open(file_path + '\\'+ file_name)
			table_list = file.Worksheets
			# 跨表复制,插入第一个表之后
			table_list('Sheet1').Copy(None,target_table_list(1))
			# 由于新表总是在第二个表,所以第二个表改名就可
			target_table_list(2).Name = file_name_dec[0].split('-',1)[0]
			file.Close(SaveChanges=0)
		else:
			print('文件夹下有非excel文件，不做处理')
	target_file.Worksheets('Sheet1').Delete()
	target_file.SaveAs(file_path + r'\处理后的文件.xlsx')
	target_file.Close(SaveChanges=0)

if __name__ == '__main__':
	file_path = r"D:\基础表"
	file= file_path + r'\处理后的文件.xlsx'
	print('正在开始汇总文件\n以下为汇总清单：')
	if os.path.exists(file):
		os.remove(file)
	file_list = get_dir(file_path)
	across_file_copy(file_path,file_list)
	print('文件汇总完毕，请查看目录下的 处理后的文件.xlsx')