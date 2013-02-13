import sqlite3 as sql
from urllib import urlretrieve
import tools
import os
import gzip
from itertools import chain
import pandas as pd

dbpath = 'data/tib.db'
gzpath = 'data/tibgz/'


class TibIO(object):
    """Class to download gzipped data when necessary"""
    def __init__(self, datelist, gz_path = gzpath):
        self.gz_path = gz_path
        self.datelist = datelist
        self._get_gz_missing_dates()


    def _download_gz(self):
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

    def _raw_data_generator(self):
        """Yields raw data based on dates in datelist"""
        for d, path in self._location_generator(self.datelist):
            yield gzip.open(path).read()

    def check_and_get(self):
        """Downloads files if necessary, returns raw gen"""
        if self.gz_missing_dates:
            self._download_gz()
        return self._raw_data_generator()

class TibPanda(object):
    """Collection of functions to interfaces pandas as parser dict"""
    def __init__(self):
        pass

    def filtered(self,d,filtstr):
        """creates filtdict by filtering subject with filtname"""
        return dict((k,v) for k,v in
                d.items() if filtstr in k) 

    def to_dataframe(self, d, name):
        timestr, datastr = self._guess_best_data(d.viewkeys())
        time = list(chain.from_iterable(d[timestr]))
        data = list(chain.from_iterable(d[datastr]))
        df = pd.DataFrame(data, index = time, columns = [name]) 
        return self._remove_redundant_data(self._remove_dup_timestamps(df))


    def _remove_dup_timestamps(self,df):
        return df.groupby(df.index).mean()

    def _remove_redundant_data(self,df):
        """Remove rows which have same values above and below"""
        tmp = df[(df.diff()!=0) | (df.diff(-1)!=0)]
        return tmp.dropna()

    def make_joined_df(self, d):
        """Takes nested dict; returns fully cleaned merged dataset"""
        dfs = [self.to_dataframe(dat, sj) for sj,dat in d.items()]
        tmp = dfs[0].join(dfs[1:], how = 'outer')
        return tmp.apply(pd.Series.interpolate,method ='time') 
        

    def _guess_best_data(self,keys):
        """Guess which time, data series we are interested in"""
        for t in ['TS','SD','TP']:
            if t in keys:
                time = t
                break
        for v in ['VP','SF','VD']:
            if v in keys:
                data = v
                break
        try:
            return time, data
        except:
            raise KeyError("Cannot find appropriate data for DataFrame")




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
