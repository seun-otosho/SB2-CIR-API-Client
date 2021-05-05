# coding: utf-8

# In[71]:


import inspect
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from os import makedirs, sep
from os.path import exists

import pandas as pd

from numpy import array_split

from ioc_cir_pro import (db, get_logger, logger_txt, pdir,
                         pdf_txt, call_live_request_dict_re, dob2s, run)

# from time import sleep
logger = get_logger()

df = pd.read_csv(
    pdir + sep + 'requests' + sep + 'batch_cir.txt', dtype=str,
    keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1"
)
done_data = run(db.fetch_all(query="""select * from requests"""))
cols = ['id', 'cust_name', 'dob', 'gender', 'bvn', 'phone', 'date_process']
ddf = pd.DataFrame(done_data, columns=cols)

df['bvn'] = df.bvn.astype(str)

ddf['bvn'] = ddf.bvn.astype(str)

df = df[~df.bvn.isin(ddf.bvn)]
df['bvn'] = df.bvn.apply(lambda x: int(x) if x not in (None, '', 'nan') else '')

df.reset_index(drop=True, inplace=True)

# _xcptn

logger.info(f"{df.shape=}\t|\t{ddf.shape=}")

# df['DOB'].fillna('01/01/1900', inplace=True)"""


try:
    df['dob'] = pd.to_datetime(df['dob'])
    df['dob'] = df.dob.dt.date
except Exception as e:
    logger.error(e)


glogger = get_logger()

df['x'] = df.index
df.fillna('', inplace=True)


df_dict_list = df[['cust_name', 'dob', 'bvn', 'gender', 'phone', 'x']].to_dict('records')


for d in df_dict_list:
    try:
        if d['dob'] != '':
            d['cust_name'] = d['cust_name'].strip()
            call_live_request_dict_re(d)
            # call_live_request_dict_re.delay(d)
    except Exception as e:
        glogger.error(e)
