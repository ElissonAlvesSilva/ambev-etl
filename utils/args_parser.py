import argparse
from datetime import date, timedelta, datetime

from utils.singleton import Singleton
from utils.system_exiter import SystemExiter


@Singleton
class ArgsParser:
    """
    Parse the arguments from the console.
    """

    def parse_arguments(self):
        # configure argument parser
        parser = argparse.ArgumentParser(
            description='ETL Ambev')
        ################## DATES ##################
        parser.add_argument(
            '-sd', '--start-date', type=str, default=None, metavar='DATE',
            help='Date (start) to run ETL for (yyyy-mm-dd). Default: Yesterday')
        parser.add_argument(
            '-ed', '--end-date', type=str, default=None, metavar='DATE',
            help='Date (end) to run ETL for (yyyy-mm-dd). Default: Equal to start-date')
        ################## STAGES ##################
        parser.add_argument(
            '-T', '--transform', action='store_true',
            help='Execute the transform step. Note that all needed dumps must have been successfully extracted to MySQL. Default: All steps true')
        parser.add_argument(
            '-L', '--load', action='store_true',
            help='Execute the load step. Note that all needed outputs must have been sucessfully transformed. Default: All steps true')
        ################## CLIENTS, INDICATORS, ETC ##################
        parser.add_argument(
            '-j', '--jobs', type=str, default=None, metavar='JOBS',
            help='Jobs to run, split by comma. Default: all jobs that are kpis')
        ################## OTHERS ##################
        parser.add_argument(
            '-e', '--env', type=str, default='dev', metavar='ENV',
            help="If the env is dev, test or prod. Default: dev")
        parser.add_argument(
            '-f', '--force-config', action='store_true',
            help='Dont ask for configuration confirmation')
        parser.add_argument(
            '-o', '--output', type=str, default=None, metavar='OUTPUT',
            help='Output files directory. Default: HOME/etl4-data/')
        parser.add_argument(
            '-p', '--partial-run', action="store_true",
            help="Boolean flag, run ETL skipping job dependencies")
        parser.add_argument(
            '-sr', '--skip-to-results', action="store_true",
            help="Boolean flag, skip jobs to just get the results.")
        parser.add_argument(
            '-v', '--verbose', action="store_true",
            help="Boolean flag, prints all the queries and their first results.")
        parser.add_argument(
            '-d', '--debug', action="store_true",
            help="Boolean flag, debug mode that get all queries results (including non-kpis queries). WARNING! Debug mode is slower than normal mode and consumes a lot of memory!")
        parser.add_argument(
            '-r', '--report', action="store_true",
            help="Boolean flag, send a report by mail after the execution")
        parser.add_argument(
            '-t', '--temp-tables', action="store_true",
            help="Boolean flag, run queries on temp tables, in order to don't overwrite the main tables. If env is not prod, it is always True.")
        parser.add_argument(
            '-l', '--temp-label', type=str, default='temp', metavar='LABEL',
            help="If using temp tables, sets the label for the temp tables. Default: _temp")
        parser.add_argument(
            '-sw', '--skip-warmup', action="store_true",
            help="Boolean flag, skip warmup. Useful when running ETL for old dumps.")
        parser.add_argument(
            '-si', '--skip-insert', action="store_true",
            help="Boolean flag, skip insert into table data. Useful when reprocess.")
        parser.add_argument(
            '-opq', '--only-print-query', action="store_true",
            help="Boolean flag, only print query used to run ETL. Useful when debug.")


        # parse arguments
        args = parser.parse_args()

        # check dates
        if args.start_date:
            args.start_date = self._check_date_format(args.start_date)
        else:
            args.start_date = date.today() - timedelta(days=1)

        if args.end_date:
            args.end_date = self._check_date_format(args.end_date)
        else:
            args.end_date = args.start_date

        if args.start_date > args.end_date:
            SystemExiter.Instance().exit('Error: Start date (' + str(args.start_date) +
                                         ') must be smaller than or equal end date (' +
                                         str(args.end_date) + ').')

        if not args.transform and not args.load:
            args.transform = True
            args.load = True

        return args

    def _check_date_format(self, date):
        try:
            return datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            SystemExiter.Instance().exit("Error: argument date: invalid format: '%s' (use yyyy-mm-dd)\n" % date)
