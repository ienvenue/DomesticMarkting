import win32com.client as win32
import datetime
import xlrd


class SendEmail:
    def __init__(self, recipient, cc, path):
        self.app = 'Outlook'
        self.recipient = recipient
        self.cc = cc
        self.path = path

    def read_excel_content(self):
        content = xlrd.open_workbook(self.path)
        table = content.sheet_by_name("文字描述")
        return table.row_values(1)[0]

    def send_outlook(self):
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
    recipients = 'ex_chenyj12@partner.midea.com'
    cc_to = 'ex_chenyj12@partner.midea.com'
    file_path = r'E:\Share\日报合计-截止20201020.xlsx'
    send_email = SendEmail(recipients, cc_to, file_path)
    send_email.send_outlook()
