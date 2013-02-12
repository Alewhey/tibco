# -*- coding: utf-8 -*-
import re
from collections import defaultdict
import tools


class TibcoParser(object):
    """Main parser object.

    Parses raw text to message objects which are appended to self.messages.
    Therefore this class is both parser and a datastructure"""
    def __init__(self, raw=None, filt_str=None, verbose=False):
        self._verbose = verbose
        self.messages = []
        if raw and filt_str:
            self.parse(raw, filt_str)

    def parse(self, raw, subject):
        """Parses raw to return lists of dates,subjects and messages"""
        self.filt_str = '.' + tools.verify_subject(subject)
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
        matchstr = r'^([\d:]+GMT):\ssubject=BMRA\.?([\w.-]*' + self.filt_str + \
            r'[\w.-]*),\smessage=\{([^}]*)\}\n'
        match = re.compile(matchstr, re.MULTILINE | re.S)
        m = re.findall(match, self._raw)
        if not m:
            raise ValueError("Error: no matches found")
        return m

    def _filter_false_positives(self, sj):
        if self.filt_str == '.INDO':
            if sj == 'SYSTEM.INDOD':
                return False
            else: return True
        elif self.filt_str == '.MEL':
            if 'INDOD' in sj:
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

    def to_simple_dict(self):
        """Flat dict for simple subject types"""
        d = defaultdict(list)
        for m in self:
            d['Date'].append(m.date)
            d['Subject'].append(m.subject)
            for k,v in m.data.items():
                d[k].extend(v)
        return d

    def to_complex_dict(self, flat = False):
        """nested dict with subject as first key"""
        d = defaultdict(lambda: defaultdict(list))
        if flat:
            for m in self:
                subd = d[m.subject]
                subd['Date'].append(m.date)
                for k,v in m.data.items():
                    subd[k].extend(v)       #difference!
        else:
            for m in self:
                subd = d[m.subject]
                subd['Date'].append(m.date)
                for k,v in m.data.items():
                    subd[k].append(v)       #difference!
        #if only 1 key, return nested dict only
        if len(d.keys()) == 1: return d[d.keys()[0]]
        else: return d



    def __repr__(self):
        return "<TibcoParser|Subject:{0}|Messages:{1}>".format(
                self.filt_str,len(self.messages))

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



        


#TODO: LOTS! Rewrite parser to work on message object. Need to find sensible
#way of storing attribues (dictionary?) and 'type' the messages
#Add methods to Parser class to deliver eg all messages in a dict etc
#Going to be a much nicer more flexible more extensible interface when finished
