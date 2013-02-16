import datetime

'''
Collection of useful functions and data
see http://www.bmreports.com/bwx_help.htm for explanation is acronyms
Fuel Types: http://www.bmreports.com/bsp/staticdata/BMUFuelType.xls
BM Units: https://downloads.elexonportal.co.uk/file/download/REGISTERED_BMUNITS_FILE?key=ydy9lxwk5cpbwdw
'''


_isostring = '%Y-%m-%d'
_subjectlist = ["MELNGC", "IMBALNGC", "TSDF", "NETBSAD", "BOALF", "DF",
        "INDOD", "DISPTAV", "FPN", "RDRE", "MID", "MEL", "NDZ", "DISBSAD",
        "TSDFD", "DISEBSP", "NDFD", "FREQ", "NDF", "BOAV", "INDO", "SYSWARN",
        "MIL", "MZT", "ITSDO", "INDDEM", "SEL", "QPN", "TEMP", "BOD",
        "TBOD", "NONBM", "RURE", "ISPSTACK", "EBOCF", "MNZT", "FUELINST", 
        "FUELHH", 'WINDFOR']

_known_subject_types = ['BM.BMUNIT.BOALF', 'BM.BMUNIT.BOAV.RANK',
        'BM.BMUNIT.BOD.RANK', 'BM.BMUNIT.DISPTAV.RANK', 'BM.BMUNIT.EBOCF.RANK', 'BM.BMUNIT.FPN', 
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

_types = { 'Date': 'timestamp', 'TS': 'timestamp', 'SD': 'timestamp', 'TP': 'timestamp',
        'SP': 'int', 'MsgID': 'int', 'NP': 'int', 'TR': 'int', 'NR': 'int', 'FG': 'int',
        'VE': 'flint', 'VP': 'flint', 'SF': 'float', 'EH': 'float', 'EL': 'float', 'EN': 'float',
        'EO': 'float', 'VG': 'float', 'TH': 'float', 'TL': 'float', 'TN': 'float',
        'TO': 'float', 'VD': 'float', 'Subject': 'string', 'SW': 'string', 'FT': 'string',
        'M1': 'float', 'M2': 'float'}

mtypedict = {
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

#------Date functions-------

def parse_date(datestr):
    return datetime.datetime.strptime(datestr, _isostring).date()


def parse_datetime(d):
    return datetime.datetime(int(d[0:4]),int(d[5:7]),int(d[8:10]),
            int(d[11:13]),int(d[14:16]),int(d[17:19]))


def date_list(dtstart, dtfin):
    dl = []
    day = datetime.timedelta(1)
    curday = dtstart
    while curday <= dtfin:
        dl.append(curday)
        curday = curday + day
    return dl

#------Typing functions etc--------
def msg_typer(key):
    """Looks in _types dict and returns typing function"""
    if key in _types:
        f = _types[key]
        if f == 'float':
            return float
        elif f == 'int':
            return int
        elif f == 'timestamp':
            return parse_datetime
        elif f == 'flint':
            return flint
    return str
    
def flint(string):
    return int(float(string))

def verify_subject(fs):
    '''Checks the subject against subject dictionary. Raises
    error if invalid.'''
    if fs.upper() in _subjectlist:
        return fs.upper()
    else:
        print "Error: invalid subject string. Must be one of following:"
        print ' | '.join(sorted(_subjectlist))
        raise IOError
