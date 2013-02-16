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
    aparser = argparse.ArgumentParser()
    aparser.add_argument("subject", type=str, help="Subject to find")
    aparser.add_argument("start_date", type=str,
            help="Date from which to start processing"
                        + " - format \'YYYY-MM-DD\'")
    aparser.add_argument("end_date", type=str,
            help="Date at which to stop processing"
                        + "- format \'YYYY-MM-DD\'")
    aparser.add_argument("dbpath", type=str, help="Path of database")
    aparser.add_argument('--verbose', 'v', action='store_true',
            help = "Verbose output flag")
    arg = aparser.parse_args()
    get_data(arg.subject, arg.start_date, arg.end_date, arg.dbpath)

def get_data(subject, start_str, end_str, dbpath = _dbpath, get_dict = False):
    #get dates
    start_date = tools.parse_date(start_str)
    end_date = tools.parse_date(end_str)
    store = iotools.TibTables(dbpath)
    #check for dates in database
    db_missing_dates = store.query_data(
            subject, start_date, end_date)
    # if data missing, check gz has been downloaded and launch processor
    if db_missing_dates:
        d = _data_to_db(db_missing_dates, subject, store)
        #TODO: Tidy this bit up - get dict??
        if get_dict:
            return d
    return store.query_data(subject,
            start_date, end_date, return_df = True)

def _data_to_db(db_missing_dates, subject, store):
    nmiss = len(db_missing_dates)
    print "Data missing from database for following dates:"
    print ', '.join([d.isoformat() for d in sorted(db_missing_dates)])
    tio = iotools.TibIO(db_missing_dates, verbose=_verbose)
    p = parser.TibParser()
    for n,raw in enumerate(tio.check_and_get()):
        sys.stdout.flush()
        sys.stdout.write("\rProcessing file %s of %s..." % (
            n + 1, nmiss))
        p.parse(raw, subject)
    tp = iotools.TibPanda()
    d = p.to_dict()
    df = tp.make_joined_df(d)
    store.append(subject,df)
    return d

if __name__ == '__main__':
    main()
