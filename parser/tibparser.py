# -*- coding: utf-8 -*-
import re
from collections import Counter


class TibcoParser(object):
    def __init__(self, raw=None, filt_str=None, verbose=False):
        self._verbose = verbose
        if raw and filt_str:
            self.parse(raw, filt_str)


    def parse(self, raw, filt_str):
        '''Takes a file and parses to return lists of dates, subjects and messages
        '''
        msgre = re.compile(r'([^=]+)=([^,$]+),?')
        self._raw = raw
        self.filt_str = filt_str
        #primary parsing
        m = self._matcher()
        #secondary parsing
        self.obs = [Message(date,subject,re.findall(msgre, msg+',')) \
                for date,subject,msg in m]


    def _matcher(self):
        matchstr = r'^([\d:]+GMT):\ssubject=BMRA\.?([\w.-]*' + self.filt_str + \
            r'[\w.-]*),\smessage=\{([^}]*)\}\n'
        match = re.compile(matchstr, re.MULTILINE | re.S)
        m = re.findall(match, self._raw)
        if not m:
            raise ValueError("Error: no matches found")
        return m


    def _remove_false_positives(self):
        blacklist = []
        if self.filt_str == '.INDO':
            for ind, entry in enumerate(self.subjects):
                if 'INDOD' in entry:
                    blacklist.append(ind)
        elif self.filt_str == '.MEL':
            for ind, entry in enumerate(self.subjects):
                if 'MELNGC' in entry:
                    blacklist.append(ind)
        if blacklist:
            self.dates = list(self.dates)
            self.subjects = list(self.subjects)
            self.mtypes = list(self.mtypes)
            self.mvals = list(self.mvals)
            for n in blacklist[::-1]:
                del self.dates[n]
                del self.subjects[n]
                del self.mtypes[n]
                del self.mvals[n]


    def find_BMU(self):
        '''Extracts the BMUNIT from a subject string

        If it fails, returns None'''
        m = re.search('\.([\w]+__?[\w-]+)\.', substr)
        if not m:
            print "Search Error!", substr
            return None
        return m.group(1)

    def increment_duplicates(self):
        '''look for duplicated entries within message types and number 1,2,3...'''
        mtypes2 = [list(line) for line in mtypes]
        for ind, line in enumerate(mtypes2):
            ct = {}
            for idx, st in enumerate(line):
                ct[st] = ct.get(st, 0) + 1
                if ct[st] > 1:
                    mtypes2[ind][idx] = mtypes2[ind][idx] + str(ct[st] - 1)
        return mtypes2

    def to_dict(self):
        # check we have more than one entry
        if not (type(self.ID) == list or type(self.ID) == tuple):
            ID, dt, sj, mt, mv = [self.ID], [self.dates],
            [self.subjects], [self.mtypes], [self.mvals]
        self._count_mtypes()
        dmsg = {}
        dinf = {'MsgID': [], 'Date': [], 'Subject': []}
        for key in self._mtype_counts:
            dmsg[key] = []
        for ind, line in enumerate(mt):
            # update info dict first
            dinf['MsgID'].append(ID[ind])
            dinf['Date'].append(dt[ind])
            dinf['Subject'].append(sj[ind])
            # if line is missing an entry, fill entry with nan
            for key in dmsg.keys():
                if not key in line:
                    dmsg[key].append(float('nan'))
            # update message dict
            for idx, st in enumerate(line):
                tmp = mv[ind][idx]
                dmsg[st].append(tmp)
            target = max([len(dmsg[key]) for key in dmsg])
            # fix lengths
            dmsg = self._fix_dict_length(dmsg, target)
            dinf = self._fix_dict_length(dinf, target)
        dmsg.update(dinf)
        return dmsg


    def _fix_dict_length(self, d, target):
        '''makes dict a consistant length by copying previous entries'''
        for key in d:
            while len(d[key]) < target:
                d[key].append(d[key][-1])
        return d

    def _count_mtypes(self):
        '''create dict of mtype strings and counts'''
        self._mtype_counts = Counter([st for line in self.mtypes for st in line])

class Message(object):
    def __init__(self, date,subject,message):
        self.date = date
        self.subject = subject
        self.message = message

        


#TODO: LOTS! Rewrite parser to work on message object. Need to find sensible
#way of storing attribues (dictionary?)
#Add methods to Parser class to deliver eg all messages in a dict etc
#Going to be a much nicer more flexible more extensible interface when finished
