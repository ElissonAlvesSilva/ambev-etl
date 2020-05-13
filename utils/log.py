import sys
from datetime import date

from config import Config
from .file_manager import FileManager
from utils.singleton import Singleton


@Singleton
class Log:
    """
    Centralizes all print to console. Generates a report with all the printed data.
    Two types of reports are written, a short and a complete version.
    """
    def __init__(self):
        self.content = ""
        self.finalreport = ""
        self.filepath = Config.WORKDIRECTORY + "logs/"
        self.filename = "log_"
        if Config.RUN_JOBS:
            self.filename += "ambev"
        self.filename += date.today().strftime("%Y-%m-%d")
        self.two_versions = True

    def append(self, message):
        if not Config.SILENCED_MODE:
            print(message, file=sys.stderr)
            sys.stdout.flush()
        self.content += message + "\n"

    def appendFinalReport(self, message):
        self.append(message)
        self.finalreport += message + "\n"

    def printFinalReport(self):
        if not Config.SILENCED_MODE:
            print(self.report(), file=sys.stderr)
            sys.stdout.flush()

    def report(self):
        return "\n*****************************\nFINAL REPORT:\n\n" + self.finalreport

    def save(self, success=True):
        status = ""
        if success == False:
            status = "_FAILED"
        if self.two_versions:
            message = self.content + self.report()
            self._save_version("complete", message, "_complete" + status)
        message = self.report()
        self._save_version("final", message, status)

    def _save_version(self, version, message, additional_name=""):
        FileManager.create_if_dont_exist(self.filepath)
        FileManager.write_string_to_file(self.filepath, self.filename + additional_name, message)
        if not Config.SILENCED_MODE:
            print("Log (" + version + " version) saved at " +\
                                 self.filepath + self.filename + additional_name, file=sys.stderr)
