import handler
import parser
import iosql
import tools
import datetime as dt

s = dt.date(2012,1,1)
e = dt.date(2012,1,6)

datelist = tools.date_list(s,e)
tio = iosql.TibIO(datelist)
if tio.gz_missing_dates:
    tio.download_gz()

p = parser.TibcoParser()
for raw in tio.raw_data_generator():
    p.parse(raw, 'FREQ')
