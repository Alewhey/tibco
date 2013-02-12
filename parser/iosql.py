import sqlite3 as sql
from urllib import urlretrieve
import tools
import os
import gzip

dbpath = 'data/tib.db'
gzpath = 'data/tibgz/'


class TibIO(object):
    """Class to download gzipped data when necessary"""
    def __init__(self, datelist, gz_path = gzpath):
        self.gz_path = gz_path
        self.datelist = datelist
        self._get_gz_missing_dates()


    def download_gz(self):
        '''Downloads tibco data from list of dates'''
        print 'Downloading TIBCO data between dates ' + self.gz_missing_dates[0].isoformat() +\
              ' and ' + self.gz_missing_dates[-1].isoformat() + \
              ' to folder:\n' + self.gz_path
        for d, path in self._location_generator(self.gz_missing_dates):
            iso = d.isoformat()
            url = 'http://www.bmreports.com/tibcodata/tib_messages.' + \
                iso + '.gz'
            print "Retrieving URL %s" % url
            try:
                urlretrieve(url, path)
            except IOError:
                print "Download failed for date " + iso + ', skipping...'
        self._get_gz_missing_dates()

    def _get_gz_missing_dates(self):
        '''Checks if dates in datelist exists in gz_path'''
        fl = os.listdir(self.gz_path)
        fs = [s[:-3] for s in fl if s.endswith('.gz')]
        self.gz_missing_dates = [d for d in self.datelist if not d.isoformat() in fs]

    def _location_generator(self, dates):
        """Generator yielding dates objects and corresponding gzip filepaths"""
        for d in dates:
            yield d, self.gz_path + d.isoformat() + '.gz'

    def raw_data_generator(self):
        """Yields raw data based on dates in datelist"""
        for d, path in self._location_generator(self.datelist):
            yield gzip.open(path).read()


class TibSQL(object):
    def __init__(self, subject, dbpath = dbpath):
        self.path = dbpath
        self.connect(dbpath)
        self.subject = subject
        self._create_subject_table()

    def connect(self, create_db=False):
        '''Checks for database existence and returns cursor object'''
        dbexist = False
        try:
            f = open(self.path)
            f.close()
            dbexist = True
        except IOError:
            pass
        if dbexist == create_db == False:
            raise IOError("Database file not found")
        elif dbexist == create_db == True:
            raise IOError("Database file already exists")
        self._con = sql.connect(self.path, detect_types=sql.PARSE_DECLTYPES)
        self._cur = sql.Cursor(self._con)
        if (dbexist == False and create_db == True):
            print "Initialising database..."
            self._create_db()

    def _create_db(self):
        query = 'CREATE TABLE processed(date DATE PRIMARY KEY,' + \
                ' BOOL NOT NULL DEFAULT 0,'.join(sorted(list(tt.subjectdict))) + \
                ' BOOL NOT NULL DEFAULT 0)'
        self._cur.execute(query)
        return

    def _create_subject_table(self):
        tojoin = []
        mtypes = tt.mtypedict[self.subject]
        for mt in mtypes:
            try:
                tojoin.append("%s %s" % (mt, tt.types[mt]))
            except KeyError:
                tojoin.append("%s var" % mt)
            joined = ','.join(tojoin)
        query = 'CREATE TABLE IF NOT EXISTS ' + self.subject + '(MsgID int,' +\
                'Date timestamp,Subject string,' + joined + ')'
        self._cur.execute(query)

    def update_processed_table(self, cur, subject, datelist):
        for d in datelist:
            cur.execute(
                'UPDATE processed SET %s = 1 WHERE date = ?' % subject, [d])
            if cur.rowcount == 0:
                cur.execute(
                    'INSERT INTO processed(date,%s) VALUES(?,1)' % subject, [d])

    def dict_dump(self, d):
        # create 'translated' list from dict
        tl = [d['MsgID'], d['Date'], d['Subject']]
        for dat in tt.mtypedict[self.subject]:
            tl.append(d[dat])
        sqllist = zip(*tl)
        nq = ','.join(['?'] * len(d))
        # create query
        query = 'INSERT INTO ' + self.subject + ' VALUES(' + nq + ')'
        self._cur.executemany(query, sqllist)

    def get_missing_dates(self, datelist):
        self._cur.execute('Select date from processed where %s = 1 and date between ? and ?'
                          % self.subject, [datelist[0], datelist[-1]])
        founddates = self._cur.fetchall()
        for d in founddates:
            datelist.remove(d[0])
        return datelist
