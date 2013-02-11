import datetime

'''
Collection of useful functions and data
see http://www.bmreports.com/bwx_help.htm for explanation is acronyms
Fuel Types: http://www.bmreports.com/bsp/staticdata/BMUFuelType.xls
BM Units: https://downloads.elexonportal.co.uk/file/download/REGISTERED_BMUNITS_FILE?key=ydy9lxwk5cpbwdw
'''


class Tools(object):
    def __init__(self):
        self._isostring = '%Y-%m-%d'
        self._subjectlist = ["MELNGC", "IMBALNGC", "TSDF", "NETBSAD", "BOALF", "DF",
            "INDOD", "DISPTAV", "FPN", "RDRE", "MID", "NDZ", "DISBSAD",
            "TSDFD", "DISEBSP", "NDFD", "FREQ", "NDF", "BOAV", "INDO",
            "MIL", "MZT", "ITSDO", "INDDEM", "SEL", "QPN", "TEMP", "BOD",
            "TBOD", "NONBM", "RURE", "ISPSTACK", "EBOCF", "MNZT", "FUELINST",

        self._known_subject_types = [
            'BM.BMUNIT.BOALF', 'BM.BMUNIT.BOAV.RANK', 'BM.BMUNIT.BOD.RANK', 
            'BM.BMUNIT.DISPTAV.RANK', 'BM.BMUNIT.EBOCF.RANK', 'BM.BMUNIT.FPN', 
            'BM.BMUNIT.MEL', 'BM.BMUNIT.MIL', 'BM.BMUNIT.QPN', 'DYNAMIC.BMUNIT.MNZT', 
            'DYNAMIC.BMUNIT.MZT', 'DYNAMIC.BMUNIT.NDZ', 'DYNAMIC.BMUNIT.NTB', 
            'DYNAMIC.BMUNIT.NTO', 'DYNAMIC.BMUNIT.RDRE', 'DYNAMIC.BMUNIT.RURE', 
            'DYNAMIC.BMUNIT.SEL', 'SYSTEM.DF.CHR', 'SYSTEM.DISBSAD', 'SYSTEM.DISEBSP', 
            'SYSTEM.FREQ', 'SYSTEM.FUELHH', 'SYSTEM.FUELINST', 'SYSTEM.IMBALNGC.CHR', 
            'SYSTEM.INDDEM.CHR', 'SYSTEM.INDGEN.CHR', 'SYSTEM.INDO', 'SYSTEM.INDOD', 
            'SYSTEM.ISPSTACK', 'SYSTEM.ITSDO', 'SYSTEM.MELNGC.CHR', 'SYSTEM.MID', 
            'SYSTEM.NDF.CHR', 'SYSTEM.NDFD', 'SYSTEM.NETBSAD', 'SYSTEM.NONBM', 
            'SYSTEM.SYSWARN', 'SYSTEM.TBOD', 'SYSTEM.TEMP', 'SYSTEM.TSDF.CHR', 
            'SYSTEM.TSDFD', 'SYSTEM.WINDFOR']

        self._types = {
            'Date': 'timestamp', 'TS': 'timestamp', 'SD': 'timestamp', 'TP': 'timestamp',
            'SP': 'int', 'MsgID': 'int', 'NP': 'int', 'TR': 'int', 'NR': 'int', 'FG': 'int',
            'VE': 'int', 'VP': 'int', 'SF': 'float', 'EH': 'float', 'EL': 'float', 'EN': 'float',
            'EO': 'float', 'VG': 'float', 'TH': 'float', 'TL': 'float', 'TN': 'float',
            'TO': 'float', 'Subject': 'string', 'SW': 'string', 'FT': 'string' }

        self.mtypedict = {
            'BOALF': ['AD', 'NK', 'NP', 'SO', 'TA', 'TS', 'VA'],
            'BOAV': ['BV', 'NK', 'NN', 'OV', 'SA', 'SD', 'SP'],
            'BOD': ['BP', 'NN', 'NP', 'OP', 'SD', 'SP', 'TS', 'VB'],
            'DF': ['BP', 'NN', 'NP', 'NR', 'OP', 'SD', 'SP', 'TP', 'TR', 'TS', 
                'VB', 'VD', 'VE', 'VF', 'VG', 'VP', 'ZI'],
            'DISBSAD': ['AI', 'JC', 'JV', 'SD', 'SO', 'SP'],
            'DISEBSP': ['A3', 'A6', 'AB', 'AO', 'BD', 'J1', 'J2', 'J3', 'J4', 
                'NI', 'PB', 'PC', 'PD', 'PP', 'PS', 'RP', 'RV', 'SD', 'SP', 'T1', 'T2'],
            'DISPTAV': ['BV', 'NN', 'OV', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'SD', 'SP'],
            'EBOCF': ['BC', 'NN', 'OC', 'SD', 'SP'],
            'FPN': ['NP', 'SD', 'SP', 'TS', 'VP'],
            'FREQ': ['SF', 'TS'],
            'FUELHH': ['FG', 'FT', 'SD', 'SP', 'TP'],
            'FUELINST': ['FG', 'FT', 'SD', 'SP', 'TP', 'TS'],
            'IMBALNGC': ['NR', 'SD', 'SP', 'TP', 'VI', 'ZI'],
            'INDDEM': ['NR', 'SD', 'SP', 'TP', 'VD', 'ZI'],
            'INDGEN': ['NR', 'SD', 'SP', 'TP', 'VG', 'ZI'],
            'INDO': ['SD', 'SP', 'TP', 'VD'],
            'INDOD': ['EH', 'EL', 'EN', 'EO', 'SD', 'TP'],
            'ISPSTACK': ['AV', 'BO', 'CF', 'CI', 'DA', 'FP', 'IP', 'IV', 'NK', 'NN', 
                'NV', 'PV', 'RI', 'SD', 'SN', 'SO', 'SP', 'TC', 'TM', 'TV'],
            'ITSDO': ['SD', 'SP', 'TP', 'VD'],
            'MEL': ['NP', 'SD', 'SP', 'TS', 'VE'],
            'MELNGC': ['NR', 'SD', 'SP', 'TP', 'VM', 'ZI'],
            'MID': ['BP', 'M1', 'M2', 'MI', 'NN', 'NP', 'OP', 'SD', 'SP', 'TS', 
                'VB', 'VE', 'VF', 'VP'],
            'MIL': ['BP', 'NN', 'NP', 'OP', 'SD', 'SP', 'TS', 'VB', 'VE', 'VF', 'VP'],
            'MNZT': ['MN', 'TE'],
            'MZT': ['MZ', 'TE'],
            'NDF': ['NR', 'SD', 'SP', 'TP', 'TR', 'VD', 'VG', 'ZI'],
            'NDFD': ['NR', 'SD', 'SP', 'TP', 'VD'],
            'NDZ': ['DZ', 'TE'],
            'NETBSAD': ['A10', 'A11', 'A12', 'A3', 'A6', 'A7', 'A8', 'A9', 'SD', 'SP'],
            'NONBM': ['NB', 'SD', 'SP', 'TP'],
            'NTB': ['DB', 'TE'],
            'NTO': ['DO', 'TE'],
            'QPN': ['NP', 'SD', 'SP', 'TS', 'VP'],
            'RDRE': ['R1', 'R2', 'R3', 'RB', 'RC', 'TE'],
            'RURE': ['TE', 'U1', 'U2', 'U3', 'UB', 'UC'],
            'SEL': ['SE', 'TE'],
            'SYSWARN': ['SW', 'TP'],
            'TBOD': ['BT', 'OT', 'SD', 'SP'],
            'TEMP': ['TH', 'TL', 'TN', 'TO', 'TP', 'TS'],
            'TSDF': ['NR', 'SD', 'SP', 'TP', 'VD', 'ZI'],
            'TSDFD': ['NR', 'SD', 'SP', 'TP', 'VD'],
            'WINDFOR': ['NR', 'SD', 'SP', 'TP', 'TR', 'VG']}

    def convert_datetime_list(self, l):
        def convert_datetime(x):
            return '%s-%s-%s %s:%s:%s' % (x[:4], x[5:7], x[8:10], x[11:13], x[14:16], x[17:19])
        return map(self.convert_datetime, l)

    def dict_typer(self, d):
        for dat in d:
            try:
                if self._types[dat] == 'timestamp':
                    d[dat] = self.convert_datetime_list(d[dat])
            except KeyError:
                pass
        return d

    def date_list(self, dtstart, dtfin):
        dl = []
        day = datetime.timedelta(1)
        curday = dtstart
        while curday <= dtfin:
            dl.append(curday)
            curday = curday + day
        return dl

    def verify_subject(self, fs):
        '''Checks the subject against subject dictionary. Raises
        error if invalid. If convert = True, returns filterstring'''
        if fs.upper() in self._subjectlist:
            return fs.upper()
        else:
            print "Error: invalid subject string. Must be one of following:"
            self.print_types()
            raise IOError

    def print_types(self):
        for x in sorted(self._subjectlist):
            print x,

    def collate_mtypes_by_subject(self):
        mtdict = {}
        date = datetime.date(2010, 01, 01)
        for sub in self._subjectlist:
            mt = set()
            print sub
            data = th.processor(date, sub, retdat=True)
            mt = set([x for y in data[3] for x in y])
            mtdict[sub] = sorted(mt)
        return mtdict

    def parse_date(self, datestr):
        return datetime.datetime.strptime(datestr, self._isostring).date()
