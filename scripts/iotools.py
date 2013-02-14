from urllib import urlretrieve
import tools
import os
import gzip
from itertools import chain
import pandas as pd
import datetime as dt

dbpath = 'data/tib.db'
gzpath = 'data/tibgz/'


class TibIO(object):
    """Class to download gzipped data when necessary"""
    def __init__(self, datelist, gz_path = gzpath, verbose = False):
        self._verbose = verbose
        self.gz_path = gz_path
        self.datelist = datelist
        self.gz_missing_dates = self._get_gz_missing_dates(self.datelist)

    def _download_gz(self):
        '''Downloads tibco data from list of dates'''
        if self._verbose:
            print 'Downloading TIBCO data between dates ' + \
                    self.gz_missing_dates[0].isoformat() +\
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
        self.gz_missing_dates = self._get_gz_missing_dates(self.datelist)

    def _get_gz_missing_dates(self, datelist):
        '''Checks if dates in datelist exists in gz_path'''
        fl = os.listdir(self.gz_path)
        fs = [s[:-3] for s in fl if s.endswith('.gz')]
        return [d for d in datelist if not d.isoformat() in fs]

    def _location_generator(self, dates):
        """Generator yielding dates objects and corresponding gzip filepaths"""
        for d in dates:
            yield d, self.gz_path + d.isoformat() + '.gz'

    def _raw_data_generator(self):
        """Yields raw data based on dates in datelist"""
        for d, path in self._location_generator(self.datelist):
            yield gzip.open(path).read()

    def check_and_get(self):
        """Convenience function. Downloads files if necessary, returns raw gen"""
        if self.gz_missing_dates:
            self._download_gz()
        return self._raw_data_generator()

class TibPanda(object):
    """Collection of functions to interface pandas and parser dict"""
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
        if len(dfs) > 1:
            tmp = dfs[0].join(dfs[1:], how = 'outer')
            return tmp.apply(pd.Series.interpolate,method ='time') 
        else:
            return dfs[0]
        
    def _guess_best_data(self,keys):
        """Guess which time, data series we are interested in"""
        for t in ['TS','TP']:
            if t in keys:
                time = t
                break
        for v in ['VP','SF','VD', 'VE','FG']:
            if v in keys:
                data = v
                break
        try:
            return time, data
        except:
            raise KeyError("Cannot find appropriate data for DataFrame")


class TibTables(pd.HDFStore):
    """Subclass of HDF5Store with extra functions for querying store"""
    def __init__(self, dbpath = 'data/data.db'):
        super(TibTables,self).__init__(dbpath)
        #if not '/master' in self.keys():
        #    self._create_master()
        
    def _create_master(self):
        #arbitrarily choose 2012
        s = dt.date(2012,1,1)
        e = dt.date(2012,12,31)
        dl = tools.date_list(s,e)
        #IMPORTANT - need to add to if need extra subjects
        #TODO: automate?
        cols = ['FPN','INDO','FREQ']
        v = [False] * len(dl)
        d = {}
        for c in cols:
            d[c] = v
        df = pd.DataFrame(d, index = dl)
        self.put('master', df, table = True)

    def query_data(self, subject, sdate, edate, return_df = False):
        """Generates list of missing dates based on query"""
        #What is best way to search for the dates..?
        if isinstance(sdate,str):
            sdate= tools.parse_date(sdate)
        if isinstance(edate,str):
            edate= tools.parse_date(edate)
        wanted_dates = set(tools.date_list(sdate,edate))
        #if subject table does not exist then all dates are missing
        if not '/' + subject in self.keys():
            return wanted_dates 
        #prepare query
        sdatetime = dt.datetime.combine(sdate,dt.time())
        edatetime = dt.datetime.combine(edate,dt.time(23,59,59))
        t1 = pd.Term('index', '>=', sdatetime)
        t2 = pd.Term('index', '<=', edatetime)
        #make query and process result
        df = self.select(subject, where=[t1,t2])
        exist_dates = set([ts.date() for ts in df.index])
        missing_dates = sorted(wanted_dates.difference(exist_dates))
        if return_df:
            if missing_dates:
                raise ValueError("Dates missing from database!")
            else:
                return df.sort()
        else:
            return missing_dates

    def get_stored_dates(self):
        d = {}
        for k in self.keys():
            df = self[k]
            d[k] = sorted(set([ts.date() for ts in df.index]))
        return d










#TODO: Improve querying system so as to stop false positives
#TODO: Link fuel types/bmu data.csv files to subjects
#TODO: Write front-end interface
