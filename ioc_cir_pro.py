""" """

import inspect
import logging
import os
from asyncio import run
from base64 import b64decode
from datetime import date, datetime
from json import dumps, loads
from logging.handlers import TimedRotatingFileHandler
from os import makedirs, sep
from os.path import exists, join, sep
from re import match

import requests
import snoop
import xmltodict
from celery import Celery
from databases import Database
from django.core.serializers.json import DjangoJSONEncoder
from fuzzywuzzy import fuzz
from heartrate import trace
from tortoise import Tortoise

from models import Ruid, Request
from test_credentials import test

trace(browser=True)


db = Database('sqlite:///db.sqlite3')

rptID = 6110
both = True

app = Celery(
    'ioc_cir_pro',
    broker='redis://localhost:6379/0',
    # backend='redis://localhost:6379/0',
    backend="db+sqlite:///db.sqlite3",
    CELERYD_MAX_TASKS_PER_CHILD=50,

)

app.conf.update(
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    cache_backend="db+sqlite:///db.sqlite3",
    database_engine_options={'echo': True},
)

url = "https://webserver.creditreferencenigeria.net/crcweb/liverequestinvoker.asmx/PostRequest"

level = logging.INFO

# pdir = os.path.abspath(os.path.join(os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir)), os.pardir))
pdir = os.path.abspath(os.path.join(os.path.abspath(__file__), os.pardir))
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent
UP_BASE_DIR = Path(__file__).resolve().parent.parent

# pdf_txt = datetime.now().strftime('%Y%b%d') + 'pdf' + datetime.now().strftime('%H')
date_str = datetime.now().strftime('%Y-%m%b-%d%a')
dateHstr = datetime.now().strftime('%Y-%m%b-%d%a %H')
user_str = test.split('&')[0].split('=')[1]

json_dir = join(UP_BASE_DIR, dateHstr, 'reports', user_str, 'JSONs')
pdf_dir = join(UP_BASE_DIR, dateHstr, 'reports', user_str, 'PDFs')
log_dir = join(UP_BASE_DIR, dateHstr, 'logs', user_str)

if not exists(json_dir):
    makedirs(json_dir)

if not exists(pdf_dir):
    makedirs(pdf_dir)

loggers = {}
# get_logger, url, pdf_dir, logger_txt

gender = {
    'M': '001',
    'MALE': '001',
    'F': '002',
    'FEMALE': '002'
}


def get_logger(logger_name=dateHstr, level=level, mini=False):
    logger_name = ''.join(logger_name.split(sep)) if logger_name and sep in logger_name else logger_name

    global loggers
    if loggers.get(logger_name):
        return loggers.get(logger_name)
    else:
        logger_name = inspect.stack()[1][3].replace('<', '').replace('>', '') if not logger_name else logger_name
        l = logging.getLogger(logger_name)
        l.propagate = False
        # formatter = logging.Formatter('%(asctime)s : %(message)s')     %(os.getpid())s|

        if mini:
            formatter = logging.Formatter('%(message)s')
        else:
            formatter = logging.Formatter(
                # '%(processName)s : %(process)s | %(threadName)s : %(thread)s:\n'
                '%(process)s - %(thread)s @ '
                '%(asctime)s {%(name)30s:%(lineno)5d  - %(funcName)23s()} %(levelname)s - %(message)s')
        # '[%(asctime)s] - {%(name)s:%(lineno)d  - %(funcName)20s()} - %(levelname)s - %(message)s')
        # fileHandler = TimedRotatingFileHandler(log_dir + '%s.log' % logger_name, mode='a')
        log_dir2use = log_dir + os.sep  # + logger_name + os.sep
        if not os.path.exists(log_dir2use): makedirs(log_dir2use)
        if l.handlers:
            l.handlers = []
        fileHandler = TimedRotatingFileHandler(log_dir2use + '%s.log' % logger_name)
        fileHandler.setFormatter(formatter)
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)

        l.setLevel(level)
        l.addHandler(fileHandler)
        l.addHandler(streamHandler)
        loggers.update(dict(name=logger_name))

        return l


logger_txt = get_logger(user_str, level, True)


# def processed(bvn):
#     recon_list = None
#     with open("recon.txt", "r") as recon:
#         recon_list = recon.read()
#
#     return True if bvn in recon_list else False


@app.task
@snoop
def call_live_request_dict_re(kwargs):
    """Process one(1) request"""
    run(Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["models"]}))

    acno, rrqst = None, None
    acname, bvn, fn, x = kwargs['cust_name'], kwargs['bvn'], kwargs['cust_name'].strip(), kwargs['x']
    kwargs['bvn'] = kwargs['bvn'] if bvn not in (None, '', 'nan') else None

    if str(fn).strip() == '':
        fn = acname
    logger = get_logger(f'{acname} - {bvn}') if bvn and bvn not in (None, '', 'nan') else get_logger(acname)
    try:
        rrqst = run(db.fetch_one(query=f"""select * from requests where bvn = {int(kwargs['bvn'])}"""))
    except:
        rrqst = None

    if rrqst:
        logger.info("\n\n\t\tTreat3D", kwargs, '\n\n', rrqst)
    else:
        if 'i' not in kwargs:
            kwargs['i'] = 0

        bvn_or_acno = bvn if bvn not in (None, '', 'nan') else acno
        pdf__f = f"{pdf_dir}{sep}{acname}.pdf" if bvn_or_acno is None else f"{pdf_dir}{sep}{acname} - {bvn_or_acno}.pdf"
        logger.info(pdf__f)
        if not exists(pdf__f):
            logger.info(dumps(kwargs, sort_keys=True, cls=DjangoJSONEncoder, indent=4))

            try:
                kwargs['dob'] = kwargs['dob'] if isinstance(
                    kwargs['dob'], date) else datetime.strptime(kwargs['dob'], "%Y-%m-%d").date()
            except:
                kwargs['dob'] = kwargs['dob'] if isinstance(
                    kwargs['dob'], datetime) else datetime.strptime(kwargs['dob'][:-9], "%Y-%m-%d").date()

            logger.info(f"kwargs['dob']\n\n\n{kwargs['dob']=}")
            # return
            rqst_dob_str, rqst_gender = (
                kwargs['dob'].strftime("%d-%b-%Y"), gender.get(kwargs['gender'].strip().upper(), '001')
            )
            #todo
            # BVN Search
            # payload = f"""{test}&strRequest={bvn_search(bvn)}"""
            # Combine Search
            # payload = f"""{test}&strRequest={combine_search(fn, rqst_gender, rqst_dob_str, acno, bvn)}"""
            # NameID Search
            payload = f"""{test}&strRequest={name_id_search(fn, rqst_dob_str, rqst_gender)}"""

            headers = {'content-type': "application/x-www-form-urlencoded"}
            logger.info(f"{'^' * 55} \nName ID search request sent for {fn}\nrequest payload is\n{payload}")
            response = requests.request("POST", url, data=payload, headers=headers)

            try:
                rrqst = run(
                    Request.create(
                        cust_name=kwargs['cust_name'], dob=kwargs['dob'].strftime("%Y-%m-%d"),
                        gender=kwargs['gender'], bvn=kwargs['bvn'], phone=kwargs['phone']
                    )
                )
            except Exception as e:
                logger.error(e)

            if 'ERROR' in response.text and 'CODE' in response.text:
                logger.error(
                    dumps(order3D2dict(xmltodict.parse(response.text)), indent=4))
            else:
                # rez = pdfRez(acname, bvn, response.text, x)

                rez, rez_code, rez_dict, pdf_rez, xml_rez = hndl_rez(fn, response, logger)

                if rez[0]:

                    if pdf_rez and pdf_rez not in ('', None):
                        try:
                            r = xml_rez['DATAPACKET']['BODY']['CONSUMER_PROFILE']['CONSUMER_DETAILS']['RUID']
                            rrqst = run(Ruid.create(bvn=rrqst, ruid=r))
                        except Exception as e:
                            logger.error(e)
                        with open(pdf__f, "wb") as fh:
                            try:
                                fh.write(b64decode(pdf_rez))
                                # if both:
                                #     with open("{}{}{} - {}.json".format(jdir, sep, acname, bvn_or_acno), 'w') as jf:
                                #         jf.write(dumps(xml_rez['DATAPACKET']['BODY'], indent=4))
                                # logger_txt.info(acname + ' - ' + bvn_or_acno)
                                logger.info(f"file {pdf__f.split(sep)[-1]} has been written to disk")
                                logger.info('#' * 88)
                                return bvn
                            except Exception as e:
                                logger.info(e)
                        return rez[1]
                else:

                    try:
                        # decide_merge(kwargs, rez[1])
                        ref, ruids = decide_merge_hyb(kwargs, rez[1])
                        if len(ruids) > 0:
                            payload = """{}&strRequest={}""".format(
                                test, merge_search(ref, ruids))
                            logger.info(f"""merged report spool request sent for {fn} using {', '.join(
                                ruids
                            )}\nrequest payload is\n{payload}""")
                        else:
                            payload = f"""{test}&strRequest={no_hit_search(ref)}"""
                            logger.info(f"""no hit report spool request sent for {fn} using {', '.join([
                                ref
                            ])}\nrequest payload is\n{payload}""")
                        response = requests.request("POST", url, data=payload, headers=headers)

                        rez, rez_code, rez_dict, pdf_rez, xml_rez = hndl_rez(fn, response, logger)

                        if rez[0]:
                            for r in ruids:
                                run(Ruid.create(bvn=rrqst, ruid=r))

                            if pdf_rez and pdf_rez not in ('', None):
                                with open(pdf__f, "wb") as fh:
                                    try:
                                        fh.write(b64decode(pdf_rez))
                                        logger_txt.info(bvn_or_acno)

                                        logger.info(f"file {pdf__f.split(sep)[-1]} has been written to disk")
                                        logger.info('#' * 88)
                                        return bvn
                                    except Exception as e:
                                        logger.info(e)
                                return rez[1]
                        # # added this for those that fail repeated with error. ..
                        else:

                            logger.warning(response.text)
                    except Exception as e:
                        logger.error(e)
                return rez[1]
            return None
        # else:
        #     logger.info('Done!')


def dob2s(x):
    # logger.info(str(x))
    pattern = "^(([1-9])|([0-9])|([0-2][0-9])|([3][0-1]))\-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\-\d{4}$".lower()
    try:
        return datetime.strptime(str(x), '%d/%m/%Y').strftime('%d-%b-%Y')
    except Exception as e:
        try:
            return datetime.strptime(str(x), '%m/%d/%Y').strftime('%d-%b-%Y')
        except Exception as e:
            return datetime.strptime(str(x), '%d-%b-%Y').strftime('%d-%b-%Y') if match(pattern,
                                                                                       str(x).lower()) else None


def combine_search(name, gender, dob, acno, bvn):
    # name = ' '.join([t for t in name.replace('.', ' ').split() if len(t) > 3])
    return f'''<REQUEST REQUEST_ID="1">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="4" SUBJECT_TYPE="1" REPORT_ID="{rptID}"/>
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


def bvn_search(bvn):
    return f'''<REQUEST REQUEST_ID="1">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS RESPONSE_TYPE="4" SUBJECT_TYPE="1" REPORT_ID="{rptID}"/>
            <INQUIRY_REASON CODE="1"/>
            <APPLICATION CURRENCY="NGN" AMOUNT="0" NUMBER="0" PRODUCT="017"/>
        </REQUEST_PARAMETERS>
        <SEARCH_PARAMETERS SEARCH-TYPE="4">
            <BVN_NO>{bvn}</BVN_NO>
        </SEARCH_PARAMETERS>
    </REQUEST>'''


def name_id_search(name, dob, gender):
    # name = ' '.join([t for t in name.replace('.', ' ').split() if len(t) > 3])
    return f'''<REQUEST REQUEST_ID="1">
        <REQUEST_PARAMETERS>
            <REPORT_PARAMETERS REPORT_ID="{rptID}" SUBJECT_TYPE="1" RESPONSE_TYPE="4" />
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


def merge_search(ref, ruids):
    if len(ruids) >= 2:
        mrgstr = """<BUREAU_ID>{}</BUREAU_ID>""".format(ruids[1])
        for r in ruids[2:]:
            try:
                mrgstr += f"""<BUREAU_ID>{r}</BUREAU_ID>"""
            except Exception as e:
                logger = get_logger(ref)
                logger.error(e)
        return f'''<REQUEST REQUEST_ID="1">
                    <REQUEST_PARAMETERS>
                        <REPORT_PARAMETERS REPORT_ID="{rptID}" SUBJECT_TYPE="1"  RESPONSE_TYPE="4"/>
                        <INQUIRY_REASON CODE="1" />
                        <APPLICATION PRODUCT="017" NUMBER="12345" AMOUNT="0" CURRENCY="NGN"/>
                        <REQUEST_REFERENCE REFERENCE-NO="{ref}">
                            <MERGE_REPORT PRIMARY-BUREAU-ID="{ruids[0]}">
                                <BUREAU_ID>{ruids[0]}</BUREAU_ID>
                                {mrgstr}
                            </MERGE_REPORT>
                        </REQUEST_REFERENCE>
                    </REQUEST_PARAMETERS>
                </REQUEST>'''
    else:
        return f'''<REQUEST REQUEST_ID="1">
                    <REQUEST_PARAMETERS>
                        <REPORT_PARAMETERS REPORT_ID="{rptID}" SUBJECT_TYPE="1"  RESPONSE_TYPE="4"/>
                        <INQUIRY_REASON CODE="1" />
                        <APPLICATION PRODUCT="017" NUMBER="12345" AMOUNT="0" CURRENCY="NGN"/>
                        <REQUEST_REFERENCE REFERENCE-NO="{ref}">
                            <MERGE_REPORT PRIMARY-BUREAU-ID="{ruids[0]}">
                                <BUREAU_ID>{ruids[0]}</BUREAU_ID>
                            </MERGE_REPORT>
                        </REQUEST_REFERENCE>
                    </REQUEST_PARAMETERS>
                </REQUEST>'''


def no_hit_search(ref):
    return f'''<REQUEST REQUEST_ID="1">
  <REQUEST_PARAMETERS>
    <REPORT_PARAMETERS REPORT_ID="{rptID}" SUBJECT_TYPE="1" RESPONSE_TYPE="4"/>
    <INQUIRY_REASON CODE="1" />
    <APPLICATION PRODUCT="001" NUMBER="12345" AMOUNT="0" CURRENCY="NGN"/>
    <REQUEST_REFERENCE REFERENCE-NO="{ref}">
      <GENERATE_NO_HIT/>
    </REQUEST_REFERENCE>
  </REQUEST_PARAMETERS>
</REQUEST>'''


def decide_merge_hyb(reqdict, d):
    logger_text = f"{reqdict['cust_name']} - {reqdict['bvn']}" if reqdict['bvn'] else reqdict['cust_name']
    logger = get_logger(logger_text)
    d, dob_ratio, x = xmltodict.parse(d), None, reqdict['i']
    ruids, ref, d = [], d['DATAPACKET']['@REFERENCE-NO'], order3D2dict(d)
    l = d['DATAPACKET']['BODY']['SEARCH-RESULT-LIST']['SEARCH-RESULT-ITEM']
    ll = len(l)
    for n, i in enumerate(l):
        logger.info(f"{n=}")
        logger.info(f"{reqdict=}")
        logger.info(dumps(i, indent=4))
        # print(i)
        # logger.info(i)        
        name_ratio = None

        sb_confscr = sb_conf_score(i, logger, ruids)

        name_ratio = name_check(i, logger, name_ratio, reqdict, ruids, sb_confscr)

        dob_check(i, logger, name_ratio, reqdict, ruids)

        phone_check(i, logger, name_ratio, reqdict, ruids)
        logger.info(f"{n}of{ll} " + f"~ {n}of{ll} " * 64)

    logger.info(ref)
    ruids = tuple(set(ruids))
    logger.info(ruids)
    logger.info("^" * 88)
    return ref, ruids


def sb_conf_score(i, logger, ruids):
    # SB Confidence Score
    try:
        sb_confscr = int(i["@CONFIDENCE-SCORE"])
        logger.info(f"""{i["@CONFIDENCE-SCORE"]=}""")
        if sb_confscr == 100:
            ruids.append(i['@BUREAU-ID'])
    except Exception as e:
        logger.error(e)
    return sb_confscr


def dob_check(i, logger, name_ratio, reqdict, ruids):
    # DOB Check
    try:
        sdob = str(datetime.strptime(i['@DATE-OF-BIRTH'], '%d-%b-%Y').date())
        dob_ratio = fuzz.token_set_ratio(str(reqdict['dob']), sdob)

        logger.info(f"""FPartR({sdob}, {str(reqdict['dob'])}) is {dob_ratio=}""")
        if name_ratio >= 94 and dob_ratio >= 90:
            ruids.append(i['@BUREAU-ID'])
    except Exception as e:
        logger.error(e)


def phone_check(i, logger, name_ratio, reqdict, ruids):
    # Phone Check
    try:
        phone_ratio = fuzz.ratio(i['@PHONE-NUMBER'][-10:], reqdict['phone'][-10:])
        logger.info(f"FRatio('{i['@PHONE-NUMBER']}', '{reqdict['phone']}') is {phone_ratio=}")
        if name_ratio >= 94 and phone_ratio == 100:
            ruids.append(i['@BUREAU-ID'])
    except Exception as e:
        logger.error(e)


def name_check(i, logger, name_ratio, reqdict, ruids, sb_confscr):
    # Name Check
    try:
        name_ratio = fuzz.token_set_ratio(i['@NAME'], reqdict['cust_name'])
        logger.info(f"FTSETR('{i['@NAME']}', '{reqdict['cust_name']}') is {name_ratio=}")
        if sb_confscr >= 92 and name_ratio >= 94:
            ruids.append(i['@BUREAU-ID'])
    except Exception as e:
        logger.error(e)
    return name_ratio


def order3D2dict(input_ordered_dict):
    return loads(dumps(input_ordered_dict))


def pdfRez(acname, resdict, logger, x):
    # logger = get_logger(acname + ' - ' + bvn)
    try:
        d = xmltodict.parse(resdict)
    except Exception as e:
        logger.info(e)
        return True, resdict
    d = order3D2dict(d)
    d = d['string']['#text']

    if 'ERROR-CODE' in d:
        ec = xmltodict.parse(d)['DATAPACKET']['BODY']['ERROR-LIST']['ERROR-CODE']
        logger.error(dumps(order3D2dict(xmltodict.parse(d)), indent=4))
        return None, ec

    if 'RESPONSE-TYPE CODE="1"' in d or 'RESPONSE-TYPE CODE="2"' in d:
        return True, d
    else:
        return False, d


def hndl_rez(fn, response, logger, x=None):
    logger.info(response.text[:128])
    rez = pdfRez(fn, response.text, logger, x)
    rez_dict = order3D2dict(xmltodict.parse(rez[1]))
    xml_rez = rez_dict['Response']['XMLResponse'] if 'Response' in rez_dict else rez_dict
    rez_code = xml_rez['DATAPACKET']['HEADER']['RESPONSE-TYPE']['@CODE']
    pdf_rez = rez_dict['Response']['PDFResponse'] if 'Response' in rez_dict else rez_dict
    return rez, rez_code, rez_dict, pdf_rez, xml_rez
