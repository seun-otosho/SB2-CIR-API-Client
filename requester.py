# %%
from os import sep
from random import choice

import pandas as pd

from ioc_cir_pro import UP_BASE_DIR, get_logger, gender

# from time import sleep
logger, smpl = get_logger(), 100


# %%
rqstID = 1


def rptID():
    return choice([6110, 6111, ])


def rspns_typ():
    return choice([1, 2, 3, ])

def combine_search(name, gender, dob, acno, bvn, rqstID):
    return f'''    <REQUEST REQUEST_ID="{rqstID}">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="{rspns_typ()}" SUBJECT_TYPE="1" REPORT_ID="{rptID()}"/>
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION CURRENCY="NGN" AMOUNT="0" NUMBER="0" PRODUCT="017"/>
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="6">
            <NAME>{name}</NAME>
            <SURROGATES>
                <GENDER VALUE="{gender}"/>
                <DOB VALUE="{dob}"/>
            </SURROGATES>
            <ACCOUNT NUMBER="{acno}"/>
            <BVN_NO>{bvn}</BVN_NO>
            <TELEPHONE_NO></TELEPHONE_NO>
        </SEARCH_PARAMETERS>
    </REQUEST>'''


def bvn_search(bvn, rqstID):
    return f'''    <REQUEST REQUEST_ID="{rqstID}">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="{rspns_typ()}" SUBJECT_TYPE="1" REPORT_ID="{rptID()}"/>
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION CURRENCY="NGN" AMOUNT="0" NUMBER="0" PRODUCT="017"/>
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="4">
            <BVN_NO>{bvn}</BVN_NO>
        </SEARCH_PARAMETERS>
    </REQUEST>'''


def name_id_search(name, dob, gender, rqstID):
    return f'''    <REQUEST REQUEST_ID="{rqstID}">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS REPORT_ID="{rptID()}" SUBJECT_TYPE="1" RESPONSE_TYPE="{rspns_typ()}" />
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION PRODUCT="0" NUMBER="0" AMOUNT="0" CURRENCY="NGN"   />
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="0">
            <NAME>{name}</NAME>
            <SURROGATES>
                <GENDER VALUE="{gender}"/>
                <DOB VALUE="{dob}"/>
            </SURROGATES>
        </SEARCH_PARAMETERS>
    </REQUEST>'''


# %%
df = pd.read_csv(
    str(UP_BASE_DIR) + sep + 'requests' + sep + 'batch_cir.txt', dtype=str,
    keep_default_na=True, sep='|', thousands=',', encoding="ISO-8859-1"
)
df = df[df.dob != 'DOB']
df['bvn'] = df.bvn.astype(str)

try:
    df['dob'] = pd.to_datetime(df['dob'])
    df['dob'] = df.dob.dt.date
except Exception as e:
    logger.error(e)

glogger = get_logger()

df['x'] = df.index
df.fillna('', inplace=True)

logger.info(df.sample().to_dict('records'))

# %%
df_dict_list = df[['cust_name', 'dob', 'bvn', 'gender',
                   'phone', 'acno', 'x']].sample(smpl).to_dict('records')  #
logger.info(choice(df_dict_list))


# %%
def compose_rqst(kwargs, i:int = 1):
    acno = None
    acname, bvn, fn, x = kwargs['cust_name'], kwargs['bvn'], kwargs['cust_name'].strip(
    ), kwargs['x']
    kwargs['bvn'] = kwargs['bvn'] if bvn not in (None, '', 'nan') else None

    rqst_gender = gender.get(kwargs['gender'].strip().upper(), '001')

    # random choice for test purposes online
    return choice([
        # bvn search
        f"""{bvn_search(bvn, i)}""",
        # name ID search
        f"""{name_id_search(fn, kwargs['dob'], rqst_gender, i)}""",
        # combine search
        f"""{combine_search(fn, rqst_gender, kwargs['dob'], acno, bvn, i)}""",
    ])


# %%
rqst_list, i = [], 1
rqst_list.append("<BULKREQUEST>")
for d in df_dict_list:
    try:
        if d['dob'] != '':
            d['cust_name'] = d['cust_name'].strip()
            d['dob']=d['dob'].strftime("%d-%b-%Y")
            rqst_str = compose_rqst(d, i)
            logger.info(f"{rqst_str=}")
            rqst_str.replace('REQUEST_ID="1"', f'REQUEST_ID="{i}"')
            rqst_list.append(rqst_str)
        i+=1
    except Exception as e:
        glogger.error(e)

rqst_list.append("</BULKREQUEST>")

# %%
with open(f"Bulk{smpl}RequestsText.xml", "w") as rqst_fyl_hndlr:
    rqst_fyl_hndlr.write('\n'.join(rqst_list))

# %%
