import pandas as pd
import numpy as np
import psycopg2
from pandas import DataFrame
from datetime import datetime, date
import collections
import re
import dataclean as dc
import sys
import traceback
import smtplib
import ConfigInfo as cf

try:
    #buildings Data
    b_url = cf.b_url
    #b_url = 'https://fakeurl'

    start = datetime.now()
    b_data = dc.jsonToFrame(b_url)

    #Cleaning Data
    b_data = dc.cleanData(b_data)

    conn = psycopg2.connect("host={0} dbname={1} user={2} password ={3} sslmode=allow".format(cf.chartio_host,cf.chartio_db,cf.chartio_id,cf.chartio_pwd))

    dc.writeFrame(conn,'pw_building',b_data)

    conn.close()
    end = datetime.now()

    print end - start
    print 'Done'

except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_log = ''.join('!! ' + line for line in lines)

    #sending email of error log
    sender = cf.gmail_id
    receivers = ['schoi@pangeare.com']
    message = """From: Linux Box <{0}>\nTo: Stan Choi <schoi@pangeare.com>\nSubject: ERROR LOG-FAIL TO WRITE {1}\n\nError message: {2}.""".format(cf.gmail_id,'pw_building', error_log)
    try:
        session = smtplib.SMTP('smtp.gmail.com',587)
        session.ehlo()
        session.starttls()
        session.ehlo()
        session.login(sender,cf.gmail_pwd)
        session.sendmail(sender, receivers, message) 
        print "Successfully sent email"
    except smtplib.SMTPException:
        print "Error: unable to send email"
