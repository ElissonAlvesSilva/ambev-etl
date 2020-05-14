import os

from config import Config
from utils.log import Log
from utils.mail_report import MailReport
from utils.singleton import Singleton


@Singleton
class SystemExiter:
    """
    Ensures that if the ETL exits unexpectedly it will be correctly finished, and that the error 
    message will be written at the log and at the mail report (if mail report is enabled).
    """

    def __init__(self):
        pass

    def exit(self, message, isThread=False):
        message = "\n<<<<<ERROR!>>>>>\n\n"+message
        Log.Instance().appendFinalReport(message)
        Log.Instance().save(success=False)
        # if Config.SEND_REPORT:
            # MailReport.Instance().run(message, " - FAILED")
        os._exit(1)
