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
    start = datetime.now()
    f = open("/home/schoi/scripts/prospect_data_dump_first_v0.11_2.14.14.sql",'r')
    query = "".join(i for i in f.read() if ord(i)<128)

    con_pangea = psycopg2.connect("host={0} dbname={1} user={2} password={3}".format(cf.pangea_api_host,cf.pangea_api_db,cf.pangea_api_id,cf.pangea_api_pwd))
    con_chartio = psycopg2.connect("host={0} dbname={1} user={2} password={3} sslmode=allow".format(cf.chartio_host,cf.chartio_db,cf.chartio_id,cf.chartio_pwd))

    cur_pangea = con_pangea.cursor()
    cur_chartio = con_chartio.cursor()

    cur_pangea.execute(query)
    data = cur_pangea.fetchall()

    column = [desc[0] for desc in cur_pangea.description]

    data2 = pd.DataFrame(data,columns=column)
    data2 = data2.applymap(lambda x: x.replace('\n','').replace('\r','').replace(';','').replace("\\","") if isinstance(x,(str, unicode)) else x)

    data2.to_csv('pcore_prospect_data'+'output.csv', sep=';', na_rep='', cols=None, header=False, index=False)
   
    cur_chartio.execute("TRUNCATE TABLE " + 'pcore_prospect_data') 
    #con_chartio.commit()
    cur_chartio.execute("DROP TABLE IF EXISTS " + 'pcore_prospect_data')
    #con_chartio.commit()
    cur_chartio.execute("CREATE TABLE " + 'pcore_prospect_data' +'()')
    con_chartio.commit()

    for column in data2.columns:
        cur_chartio.execute("ALTER TABLE " + 'pcore_prospect_data' +" ADD COLUMN " + column.lower() + " text;")
        con_chartio.commit()

    columnString = ','.join(data2.columns)

    cur_chartio.copy_from(open('pcore_prospect_data'+'output.csv','r'), 'pcore_prospect_data', sep=';', null='NA', columns=None)
    con_chartio.commit()
  
    cur_chartio.execute("GRANT ALL ON TABLE " + 'pcore_prospect_data' +" TO GROUP reporting_role;")
    con_chartio.commit()
    
    f2 = open("/home/schoi/scripts/prospect_data_dump_second_V0.4_2.4.14_sc.sql",'r')
    query2 = "".join(i for i in f2.read() if ord(i)<128)
    
    cur_chartio.execute("DROP TABLE IF EXISTS " + 'analytics.prospects')
    cur_chartio.execute("CREATE TABLE " + "analytics.prospects AS " + query2 + ";") 
    cur_chartio.execute("GRANK ALL ON TABLE " + "analytics.prospects TO GROUP reporting_role;")
    con_chartio.commit()

    cur_pangea.close()
    cur_chartio.close()
    con_pangea.close()
    con_chartio.close()

    print datetime.now() - start
    print 'Done'
except:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
    error_log = ''.join('!! ' + line for line in lines)

    #sending email of error log
    sender = cf.gmail_id
    receivers = [cf.recip]
    message = """From: Linux Box <{0}>\nTo:<{1}>\nSubject: ERROR LOG-FAIL TO WRITE {2}\n\nError message: {3}.""".format(cf.gmail_id,cf.recip,'pcore_prospect', error_log)
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
