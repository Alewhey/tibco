import handler
import parser
import iosql
import tools
import datetime as dt
import pandas as pd

s = dt.date(2012,1,1)
e = dt.date(2012,1,10)

datelist = tools.date_list(s,e)
tio = iosql.TibIO(datelist)
if tio.gz_missing_dates:
    tio.download_gz()

p = parser.TibcoParser()
for raw in tio.raw_data_generator():
    p.parse(raw, 'fpn')

d = p.to_complex_dict(flat=True)
drax = [d[key] for key in d if 'DRAX' in key] 

dpow = [pd.DataFrame(dat['VP'],dat['TS'],['VS'+str(ix)]) for ix,dat in enumerate(drax)]

df = dpow[0].join(dpow[1:])
df2 = df.apply(pd.Series.interpolate,method='time') 

print "try pylab ; df2.plot()!"
