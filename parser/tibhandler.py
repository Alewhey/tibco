import tibparser as tp
import tibio
import argparse
import tibtools as tt
import gzip
import sys
tools = tt.Tools()


def main():
    '''parse command line arguments and send handler'''
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", type=str,
                        help="Date from which to start processing"
                        + " - format \'YYYY-MM-DD\'")
    parser.add_argument("end_date", type=str,
                        help="Date at which to stop processing - format \'YYYY-MM-DD\'")
    parser.add_argument("subject", type=str,
                        help="Subject to find")
    parser.add_argument("dbpath", type=str,
                        help="Path of database")
    parser.add_argument(
        "-n", "--newdb", help="Create new database flag", action='store_true')
    arg = parser.parse_args()
    handler(tools.parse_date(arg.start_date), tools.parse_date(arg.end_date),
            arg.subject, arg.dbpath, arg.newdb)


def handler(start, fin, subject, dbpath, newdb):
    '''Call various functions to download files (if necessary),
    unzip, concatenate, filter, parse and save to SQLite.'''
    # handler should mainly just call functions and print outcomes,
    # not process data.
    subject = tools.verify_subject(subject)
    filt_str = '.' + subject
    datelist = tools.date_list(start, fin)
    # connect to database and check if data exists
    tsql = TibSQL(subject, dbpath)
    sql_missing_dates = tsql.get_missing_dates(datelist)
    # if data missing, check gz has been downloaded and launch processor
    if sql_missing_dates:
        nmiss = len(sql_missing_dates)
        print "Data missing from database for following dates:"
        print ', '.join([d.isoformat() for d in sql_missing_dates])
        # Create TibIO object to handle gz downloads
        tio = tibio.TibIO(tools.gzloc, datelist)
        if tio.missing_gz:
            tio.download_gz()
        for raw in tio.get_raw_data():
            sys.stdout.write("\rProcessing file %s of %s..." % (n + 1, nmiss))
            sys.stdout.flush()
            p = tp.TibcoParser(raw, subject)
        # save dict to sql database
            tsql.dict_dump(subject, d)
        # update processed table
        tsql.update_processed_table(cur, subject, datelist)
        con.commit()
        print "\nProcessing complete, database updated. %s files added" % nmiss
    else:
        print "Data already exists in database. No action performed."


def print_types():
    for x in sorted(subjectdict):
        print x,

if __name__ == '__main__':
    main()
