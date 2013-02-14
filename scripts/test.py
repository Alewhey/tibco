import parser
import iotools
import tools
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
"""
s = dt.date(2012,1,1)
e = dt.date(2012,1,2)

datelist = tools.date_list(s,e)
tio = iosql.TibIO(datelist)

p = parser.TibParser()
for raw in tio.check_and_get():
    p.parse(raw, 'FPN')

d = p.to_dict()
tp = iosql.TibPanda()
d = tp.filtered(d, 'DRAX')
df = tp.make_joined_df(d)
"""

#dummy data
s = dt.datetime(2012,1,1,11,22,33)
e = dt.datetime(2012,1,1,11,22,33) + (dt.timedelta(1) * 99)
d = {'Date': tools.date_list(s,e), 'A':list(xrange(100)), 'B': list(xrange(100,0,-1))}
df = pd.DataFrame(d)
df.set_index('Date', inplace=True)
store = iotools.TibTables('data/store.h5')



