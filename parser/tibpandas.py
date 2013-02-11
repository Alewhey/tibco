import pandas as pd
import pandas.io.sql as psql
import sqlite3 as sql
import datetime as dt
import numpy as np


def connect(path):
    return sql.connect(path, detect_types=sql.PARSE_DECLTYPES)


def retrive_data(con, subject, subfilt=None):
    if subfilt:
        df = psql.read_frame("select * from %s where subject like '%%%s%%'" %
                            (subject, subfilt), con)
    else:
        df = psql.read_frame("select * from %s" % subject, con)
    return df


def fpn_format(df):
    df = df.sort('Date')
    if len(set(df.Subject)) == 1:
        df = df.drop('Subject', axis=1)
    else:
        print "multiple subjects detected", set(df.Subject)
    # remove row if VP and TS do not change but MsgID does
    msglog = df.MsgID.diff() != 0
    vplog = df.VP.diff() == 0
    tslog = df.TS.diff() == dt.timedelta(0)
    l = msglog * vplog * tslog
    df = df[~l]
    return df
