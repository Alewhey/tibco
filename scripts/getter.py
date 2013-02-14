import parser
import iotools 
import argparse
import tools
import sys

_verbose = False
_dbpath = 'data/data.db'

def main():
    global _verbose
    '''parse command line arguments and send handler'''
    parser = argparse.ArgumentParser()
    parser.add_argument("subject", type=str, help="Subject to find")
    parser.add_argument("start_date", type=str,
            help="Date from which to start processing"
                        + " - format \'YYYY-MM-DD\'")
    parser.add_argument("end_date", type=str,
            help="Date at which to stop processing"
                        + "- format \'YYYY-MM-DD\'")
    parser.add_argument("dbpath", type=str, help="Path of database")
    parser.add_argument('--verbose', 'v', action='store_true',
            help = "Verbose output flag")
    arg = parser.parse_args()
    TibGetter(arg.subject, arg.start_date, arg.end_date, arg.dbpath)

class TibGetter(object):
    def __init__(self, subject, start_str, end_str, dbpath = _dbpath):
        self.subject = tools.verify_subject(subject)
        self.start_date = tools.parse_date(start_str)
        self.end_date = tools.parse_date(end_str)
        self.dbpath = dbpath

    def get_data(self):
        #get dates
        datelist = tools.date_list(self.start_date,self.end_date)
        self.store = iotools.TibTables(self.dbpath)
        #check for dates in database
        db_missing_dates = self.store.query_data(
                self.subject, self.start_date, self.end_date)
        # if data missing, check gz has been downloaded and launch processor
        if db_missing_dates:
            self._data_to_db(db_missing_dates)
        return self.store.query_data(self.subject,
                self.start_date, self.end_date, return_df = True)

    def _data_to_db(self, db_missing_dates):
        #takes
        nmiss = len(db_missing_dates)
        print "Data missing from database for following dates:"
        print ', '.join([d.isoformat() for d in sorted(db_missing_dates)])
        tio = iotools.TibIO(db_missing_dates, verbose=_verbose)
        p = parser.TibParser()
        for n,raw in enumerate(tio.check_and_get()):
            sys.stdout.flush()
            sys.stdout.write("\rProcessing file %s of %s..." % (
                n + 1, nmiss))
            p.parse(raw, self.subject)
        tp = iotools.TibPanda()
        df = tp.make_joined_df(p.to_dict())
        self.store.append(self.subject,df)

if __name__ == '__main__':
    main()
