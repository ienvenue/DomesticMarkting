import win32com.client as win32
import datetime
import xlrd
from win32com.client import Dispatch


class SendEmail:
    def __init__(self, recipient, cc, path):
        """
        初始化发送邮件信息
        :param recipient:收件人名单
        :param cc:抄送对象名单
        :param path:附件地址，单附件
        """
        self.app = 'Outlook'
        self.recipient = recipient
        self.cc = cc
        self.path = path

    def read_excel_content(self):
        """
        以字符串的形式返回excel表格的内容
        :return:读取附件中的文本内容
        """
        content = xlrd.open_workbook(self.path)
        table = content.sheet_by_name("文字描述")
        return table.row_values(1)[0]

    def just_open(self):
        """
        打开文件并保存推出
        """
        app = Dispatch("Excel.Application")
        app.Visible = False
        content = app.Workbooks.Open(self.path)
        content.Save()
        content.Close()

    def send_outlook(self):
        """
        发送邮件函数
        """
        self.just_open()
        email = win32.gencache.EnsureDispatch("%s.Application" % self.app)
        mail = email.CreateItem(win32.constants.olMailItem)
        # 收件人
        mail.To = self.recipient
        # 抄送
        mail.CC = self.cc
        mail.Subject = '截至 ' + str(datetime.datetime.now())[0:10] + ' 零售日报'  # 邮件主题
        mail.Attachments.Add(self.path, 1, 1, "零售日报")  # 附件
        mail.Body = self.read_excel_content()
        mail.Send()


if __name__ == '__main__':
    # recipients = '''
    #   Yifeng Huang 黄一峰 <yifeng.huang@midea.com>; Kaifei Su 苏凯飞 <sukf@midea.com>; Mack 毛鑫 <maoxin@midea.com>;
    #   Xiaodong Zhu 朱晓东 <zhuxd@midea.com>; Sawyer 孙昱寰 <sunyh3@midea.com>; Haifeng Liang 梁海峰 <lianghf5@midea.com>;
    #   Rick Chen 陈可力 <keli.chen@midea.com>; Xin Liu 刘鑫 <liuxin17@midea.com>; Xiaohong Feng 冯小红 <fengxh3@midea.com>;
    #   Jinlan Chen 陈锦兰 <chenjl3@midea.com>; Guangming Wang 王广明 <wanggm2@midea.com>; Yan Cao 曹岩 <caoyan1@midea.com>;
    #   Peize Sun 孙佩泽 <peize.sun@midea.com>; felix 万方 <wanfang@midea.com>; Shuo Cui 崔硕 <shuo.cui@midea.com>;
    #   Suzette 张雪钰 <zhangxy79@midea.com>; Wei Liang 梁炜 <wei1.liang@midea.com>; Yuntao Jia 郏云涛 <jiayt@midea.com>;
    #   Dongdong Yang 杨冬冬 <dongdong.yang@midea.com>; Zhe Li 李哲 <zhe2.li@midea.com>; Jing Chen 陈静 <chj@midea.com>;
    #   Feng Pan 潘峰 <panfeng@midea.com>; Zhichao Wang 王志超 <wangzc2@midea.com>; Karen 张可然 <zhangkr@midea.com>;
    #   Binyang Wang 王斌阳 <binyang.wang@midea.com>; Hui Xu 徐惠 <hui3.xu@midea.com>; 堵维伟 <duww3@midea.com>;
    #   Zheng Gong 宫正 <gongzheng@midea.com>; hongyu 洪宇 <hongyu@midea.com>; Hugo 于国新 <guoxin.yu@midea.com>;
    #   Gene 吉九燃 <jijr1@midea.com>; Fuxing Ding 丁付行 <dingfx@midea.com>; DANGHUI CHEN 陈党辉 <chendh14@midea.com>;
    #   Jenny 齐娟<qijuanxyj@midea.com>;Hongmei Wang 王红梅<hongmei2.wang@midea.com>;gu mingli 顾明丽<mingli.gu@midea.com>;
    #   yuan na na 原娜娜<nana1.yuan@midea.com>;Li Chen 陈莉<chenli@midea.com>;Zhi Chen 陈志<zhi1.chen@midea.com>;
    #   Tony Chan 陈涛<chentao@midea.com>;wubin.wang@midea.com
    # '''
    # cc_to = 'Louis 赵磊<zhaolei2@midea.com>;Chunkai Wang 王春凯<wangck1@midea.com>'
    recipients = '''  ex_chenyj12@partner.midea.com '''
    cc_to = '''  ex_chenyj12@partner.midea.com '''
    file_path = r'\\10.157.2.94\共享文件\固定报表\日报\零售日报.xlsx'
    send_email = SendEmail(recipients, cc_to, file_path)
    send_email.send_outlook()
