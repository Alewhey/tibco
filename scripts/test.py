import parser
import iotools
import tools
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
import getter

#for sj in ['freq','fpn','mel','indo']:
tg = getter.TibGetter('fuelinst','2012-01-01','2012-01-01')
df = tg.get_data()




