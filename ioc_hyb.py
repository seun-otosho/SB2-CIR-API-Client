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

df.reset_index(drop=True, inplace=True)

# _xcptn

logger.info(f"{df.shape=}\t|\t{ddf.shape=}")

# df['DOB'].fillna('01/01/1900', inplace=True)"""


try:
    df['dob'] = df['dob'].apply(lambda x: dob2s(x))
except Exception as e:
    logger.error(e)

"""# def file_logger(acctname):
#     logger_txt = get_logger(pdf_txt)
#     with open(pdf_txt, "a") as myfile:
#         try:
#             myfile.write(acctname + '\n')
#         except Exception as e:
#             sleep(10)
#             myfile.write(acctname + '\n')
"""

# In[97]:

glogger = get_logger()


"""#
# def decide_merge(reqdict, d):
#     logger = get_logger(reqdict['CUST_NAME'] + ' - ' + reqdict['BVN'])
#     d, x = xmltodict.parse(d), reqdict['x']
#     d = order3D2dict(d)
#     ld, ll, ua = {}, [], []
#     ref = d['DATAPACKET']['@REFERENCE-NO']
#     l = d['DATAPACKET']['BODY']['SEARCH-RESULT-LIST']['SEARCH-RESULT-ITEM']
#     prev_max_name, prev_max_dob, pruid, ruids = None, None, None, []
#     for n, i in enumerate(l):
#         logger.info(dumps(i, indent=4))
#
#         # Name merge
#         try:
#             if fuzz.partial_token_set_ratio(i['@NAME'], reqdict['CUST_NAME']) == 100:
#                 ruids.append(i['@BUREAU-ID'])
#         except Exception as e:
#             logger.error(e)
#
#         try:
#             if fuzz.partial_ratio(i['@PHONE-NUMBER'], reqdict['phone']) >= 95:
#                 ruids.append(i['@BUREAU-ID'])
#         except Exception as e:
#             logger.error(e)
#         dob_ratio = fuzz.partial_ratio(i['@DATE-OF-BIRTH'], reqdict['dob'])
#
#         try:
#             ld[str(dob_ratio)] = i['@BUREAU-ID']
#         except Exception as e:
#             logger.error(e)
#
#         # if len(ll) == 0:
#         #     if any(li == dob_ratio for li in ll):
#         ua.append(i['@BUREAU-ID'])
#         try:
#             if dob_ratio >= 64:
#                 ll.append(dob_ratio)
#         except Exception as e:
#             logger.error(e)
#
        # logger.info(
        #     "{} compared with {} is {}".format(
        #         i['@DATE-OF-BIRTH'], reqdict['dob'], dob_ratio
        #     )
        # )
#
#         if dob_ratio == 100:
#             pruid = i['@BUREAU-ID']
#         if dob_ratio >= 91 and pruid is None:
#             pruid = i['@BUREAU-ID']
#         if dob_ratio == 82 and pruid is None:
#             pruid = i['@BUREAU-ID']
#
#         if (dob_ratio == 100
#             or dob_ratio >= 91
#             or dob_ratio in (82, 64, 55)):
#             ruids.append(i['@BUREAU-ID'])
#
#     if pruid is None:
#         try:
#             pruidi = max(ll)
#             pruid = ld[str(pruidi)]
#         except Exception as e:
#             logger.error(e)
#
#     if len(ruids) < 2:
#         if len(ua) > 3:
#             ruids = ll.sort(reverse=True)[:3]
#         else:
#             ruids = ua
#
#     ruids = set(ruids)
#
#     if pruid is not None:
#         while pruid in ruids:
#             ruids.remove(pruid)
#
#     if pruid is not None:
#         ruids = tuple([pruid] + list(set(ruids)))
#     else:
#         ruids = tuple(set(ruids))
#
#     logger.info(ref)
#     logger.info(ruids)
#     return ref, ruids
#

# df['rezstr'] = df[
#     ['ACCOUNTNAME', 'ACCTNO', 'FULLNAME', 'dob', 'BVN', 'SEX']
# ].apply(lambda x: call_live_request(*x), axis=1)  # .head(1)


# def record_processed(bvn):
#     if bvn:
#         with open("recon.txt", "a") as myfile:
#             myfile.write(f"{bvn}")

#         return bvn
#     return None"""


df['x'] = df.index
df.fillna('', inplace=True)

"""# dfs = array_split(df, 4)
# df = dfs[3]
# df = df.sample(10, replace=False)"""
df_dict_list = df[['cust_name', 'dob', 'bvn', 'gender', 'phone', 'x']].to_dict('records')
# .sample(5).tail(5000)
# sample(5, replace=False).
"""# df_dict_list = df[['ACCOUNTNAME', 'ACCTNO', 'CUST_NAME',
#    'dob', 'BVN', 'Gender', 'phone', 'x']].to_dict('records')
# pool = mp.Pool(processes=100)
# max_workers=1, pool.map(call_live_request_dict, df_dict_list)

# with futures.ThreadPoolExecutor(thread_name_prefix='BatchLiveRequest') as executor:  # max_workers=100
#     executor.map(call_live_request_dict, df_dict_list)


# recon_list = [call_live_request_dict.delay(d) for d in df_dict_list]

# # while True:
# for bvn in recon_list:
#     wbvn = bvn.wait()
#     # record_processed(wbvn)
#     print(wbvn)
#     glogger.info(wbvn)"""

for d in df_dict_list:
    try:
        if d['dob'] != '':
            d['cust_name'] = d['cust_name'].strip()
            # call_live_request_dict_re(d)
            call_live_request_dict_re.delay(d)
    except Exception as e:
        glogger.error(e)

"""# for row in df.iterrows():
#     call_live_request(row['ACCOUNTNAME'], row['ACCTNO'], row['FULLNAME'], row['dob'], row['BVN'])"""
# In[61]:


# df['lrezstr'] = df['rezstr'].apply(lambda x: len(x))  # .head(1)
