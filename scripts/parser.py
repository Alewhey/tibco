# -*- coding: utf-8 -*-
import re
from collections import defaultdict
import tools
import datetime

class TibParser(object):
    """Main parser object.

    Parses raw text to message objects which are appended to self.messages.
    Therefore this class is both parser and a datastructure"""
    def __init__(self, raw=None, subject=None, verbose=False):
        self._verbose = verbose
        self.messages = []
        self.dp = DictParser()
        if raw and subject:
            self.parse(raw, subject)

    def parse(self, raw, subject):
        """Parses raw to return lists of dates,subjects and messages"""
        self.subject = tools.verify_subject(subject)
        msgre = re.compile(r'([^=]+)=([^,$]+),?')
        self._raw = raw
        #primary parsing
        m = self._matcher()
        #secondary parsing
        for date, sj, msg in m:
            if self._filter_false_positives(sj):
                self.messages.append(Message(
                    date, sj, re.findall(msgre, msg+',')))

    def _matcher(self):
        matchstr = r'^([\d:]+GMT):\ssubject=BMRA\.?([\w.-]*\.'+self.subject+\
            r'[\w.-]*),\smessage=\{([^}]*)\}\n'
        match = re.compile(matchstr, re.MULTILINE | re.S)
        m = re.findall(match, self._raw)
        if not m:
            raise ValueError("Error: no matches found")
        return m

    def _filter_false_positives(self, sj):
        if self.subject == 'INDO':
            if sj == 'SYSTEM.INDOD':
                return False
            else: return True
        elif self.subject == 'MEL':
            if 'MELNGC' in sj:
                return False
            else: return True
        else: return True

    def get_data_types(self):
        """Print all data names and types"""
        s = set()
        for m in self:
            for key in m.data:
                s.add(key)
        for dat in sorted(s):
            print dat, tools._types[dat]

    def gather(self, dat):
        dat = dat.upper()
        return [m.data[dat] for m in self]

    def to_dict(self):
        return self.dp.convert(self.messages, self.subject)

    def __repr__(self):
        return "<TibcoParser|Subject:{0}|Messages:{1}>".format(
                self.subject,len(self.messages))

    def __iter__(self):
        for m in self.messages:
            yield m

    def __getitem__(self,n):
        return self.messages[n]

    def __len__(self):
        return len(self.messages)


class Message(object):
    def __init__(self, date,subject,message):
        self.date = tools.parse_datetime(date)
        self.subject = subject
        self.data = self._data_to_dict(message)

    def _data_to_dict(self,msg):
        d = defaultdict(list)
        for mt, mv in msg:
            d[mt].append(tools.msg_typer(mt)(mv))
        return d

    def __repr__(self):
        return '<Message {0}|Date {1}>'.format(
                self.subject, self.date.isoformat())

class DictParser(object):
    """Class to turn parser object into dict based on 
    subject and message characteristics"""
    def __init__(self):
        self._parser_dict = {
                'FPN':self._generic,
                'FREQ':self._generic,
                'MEL':self._generic,
                'INDO':self._generic,
                'FUELINST':self._fuel,
                'FUELHH':self._fuel,
                'MID':self._market
                }
        self._data_dict = {
                'FPN': ['TS','VP'],
                'FREQ': ['TS','SF'],
                'MEL': ['TS','VE'],
                'MIL': ['TS','VF'],
                'INDO': ['TP','VD']
                }

    def convert(self, msgs, sj):
        """Convenience function"""
        self.msgs = msgs
        self.sj = sj
        try:
            d = self._parser_dict[sj]() 
        except KeyError:
            d = self._generic()
        return d

    def _generic(self):
        """Seems to work with FPN, ??MEL??, INDO, FREQ"""
        d = defaultdict(lambda: defaultdict(list))
        indexkey,datakey = self._data_dict[self.sj]
        for m in self.msgs:
            subd = d[m.subject]
            subd['index'].append(m.data[indexkey])
            subd['data'].append(m.data[datakey])
        return d

    def _fuel(self):
        d = defaultdict(lambda: defaultdict(list))
        for m in self.msgs:
            subd = d[m.data['FT'][0]]
            subd['index'].append(m.data['TS'])
            subd['data'].append(m.data['FG'])
        return d

    def _market(self):
        sp = datetime.timedelta(0,60*30)
        d = defaultdict(lambda: defaultdict(list))
        for m in self.msgs:
            subd = d[m.data['MI'][0]]
            dt = m.data['SD'][0] + m.data['SP'][0]*sp
            subd['index'].append([dt])
            subd['data'].append(m.data['M1'])
        return d


