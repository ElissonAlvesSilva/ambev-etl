import smtplib
import time
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPException

from config import Config
from utils.log import Log
from utils.singleton import Singleton


@Singleton
class MailReport:
    """
    Encapsulates functionalities to send an email.
    """

    def __init__(self):
        self.title = "[ETL4] Report for " + date.today().strftime("%Y-%m-%d") + " (AMBEV)"
        self.config_message = ""

    def configure(self, config_message):
        self.config_message = config_message

    def run(self, message, title_label=""):
        print("\nSending mail report...")
        full_message = self._generate_message(message)
        for dest in Config.SEND_REPORT_TO:
            test = False
            cont = 1
            while test is False:
                test = True
                try:
                    self._send_mail(full_message, dest, title_label)
                except SMTPException:
                    if cont == 21:
                        raise Exception("<<< Tried 20 times to SEND MAIL REPORT and failed, ABORTING! >>>")
                    Log.Instance().appendFinalReport("<<< EXCEPTION SMTPException, RETRY! (" + str(cont) + " time)>>>")
                    test = False
                    cont += 1
                    time.sleep(60)
        print("Mail report sent.")

    def _generate_message(self, custom_message):
        message = """<html><body>"""
        if self.config_message != "":
            message += "<b>CONFIGURATION:</b><br>"
            message += self.config_message.replace('\n', '<br>') + "<br><br>"
        message += "<b>EXECUTION INFO:</b><br>"
        message += custom_message.replace("\n", "<br>")
        message += """</html></body>"""
        return message

    def _send_mail(self, message, toaddrs, title_label):
        fromaddr = 'news@linx3.com'

        msg = MIMEMultipart('alternative')
        title_temp = self.title
        msg['Subject'] = title_temp + title_label
        msg['From'] = fromaddr
        msg['To'] = toaddrs

        part1 = MIMEText(message, 'html')
        msg.attach(part1)
          
        # Credentials (if needed)  
        username = 'news@linx3.com'
        password = '4chaos1order?'
          
        # The actual mail send
        server = smtplib.SMTP('smtp.gmail.com:587')  
        server.starttls()
        server.login(username, password)
        server.sendmail(fromaddr, toaddrs, msg.as_string())  
        server.quit()
