import parser
import iotools
import tools
import datetime as dt
import pandas as pd
import getter
import unittest

class ParserTest(unittest.TestCase):

    def setUp(self):
        self.p = parser.TibParser()
        self.fpnstr = \
            "2012:01:01:04:00:44:GMT: subject=BMRA.BM.T_BARKB2.FPN, message={"\
            "SD=2012:01:01:00:00:00:GMT,SP=11,NP=2,TS=2012:01:01:05:00:00:GMT"\
            ",VP=5.0,TS=2012:01:01:05:30:00:GMT,VP=0.0}\n"
        self.indostr = \
            "2012:01:01:04:30:40:GMT: subject=BMRA.SYSTEM.INDO, message={"\
            "TP=2012:01:01:04:30:00:GMT,SD=2012:01:01:00:00:00:GMT,SP=9,"\
            "VD=23779.0}\n"
        self.melstr = \
            "2012:01:01:04:31:02:GMT: subject=BMRA.BM.T_ABTH7.MEL, message"\
            "={SD=2012:01:01:00:00:00:GMT,SP=12,NP=2,TS=2012:01:01:05:30:00:GMT"\
            ",VE=8.1,TS=2012:01:01:06:00:00:GMT,VE=1.0}\n"
        self.fpn_wrong_type = \
            "2012:01:01:04:00:44:GMT: subject=BMRA.BM.T_BARKB2.FPN, message={"\
            "SD=2012:01:01:00:00:00:GMT,SP=11,NP=2,TS=2012:01:01:05:00:00:GMT"\
            ",VP=string_not_float,TS=2012:01:01:05:30:00:GMT,VP=0.0}\n" #note VP

    def test_parse_fpn(self):
        self.p.parse(self.fpnstr,'fpn')
        d = dt.datetime(2012,1,1,4,0,44)
        sj = 'BM.T_BARKB2.FPN'
        vp = [5,0]
        m = self.p.messages[0]
        self.assertEqual(d,m.date)
        self.assertEqual(sj,m.subject)
        self.assertEqual(vp,m.data['VP'])

    def test_parse_indo(self):
        self.p.parse(self.indostr,'indo')
        d = dt.datetime(2012,1,1,4,30,40)
        sj = 'SYSTEM.INDO'
        vd = [23779]
        m = self.p.messages[0]
        self.assertEqual(d,m.date)
        self.assertEqual(sj,m.subject)
        self.assertEqual(vd,m.data['VD'])

    def test_parse_mel(self):
        self.p.parse(self.melstr,'mel')
        d = dt.datetime(2012,1,1,4,31,2)
        sj = 'BM.T_ABTH7.MEL'
        ve = [8,1]
        m = self.p.messages[0]
        self.assertEqual(d,m.date)
        self.assertEqual(sj,m.subject)
        self.assertEqual(ve,m.data['VE'])

    def test_parse_wrong_type(self):
        self.assertRaises(ValueError, self.p.parse, self.fpn_wrong_type,'fpn')


if __name__ == '__main__':
    unittest.main()

