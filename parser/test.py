import handler
import parser
import iosql
import tools
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt

s = dt.date(2012,1,1)
e = dt.date(2012,1,2)

datelist = tools.date_list(s,e)
tio = iosql.TibIO(datelist)

p = parser.TibcoParser()
for raw in tio.check_and_get():
    p.parse(raw, 'FPN')

d = p.to_dict()
tp = iosql.TibPanda()
d = tp.filtered(d, 'T_')
df = tp.make_joined_df(d)



"""
dpow = [pd.DataFrame(dat['VP'],dat['TS'],['VS'+str(ix)]) for ix,dat in enumerate(drax)]

df = dpow[0].join(dpow[1:])
df2 = df.apply(pd.Series.interpolate,method='time') 

df2.plot()

plt.show()"""
