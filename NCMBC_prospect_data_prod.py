import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email import Encoders
import os
import psycopg2 as pg
import pandas as pd
from pandas import ExcelWriter
import ConfigInfo as config

def mail(to, subject, text, attach):
   msg = MIMEMultipart()

   msg['From'] = config.gmail_id
   msg['To'] = to
   msg['Subject'] = subject

   msg.attach(MIMEText(text))

   fileMsg = MIMEBase('application','vnd.ms-excel')
   fileMsg.set_payload(file('output.xlsx').read())
   Encoders.encode_base64(fileMsg)
   fileMsg.add_header('Content-Disposition','attachment;filename=output.xlsx')
   msg.attach(fileMsg)

   mailServer = smtplib.SMTP("smtp.gmail.com", 587)
   mailServer.ehlo()
   mailServer.starttls()
   mailServer.ehlo()
   mailServer.login(config.gmail_id, config.gmail_pwd)
   mailServer.sendmail(config.gmail_id, to, msg.as_string())
   # Should be mailServer.quit(), but that crashes...
   mailServer.close()

con = pg.connect("host={0} dbname={1} user={2} password={3}".format(config.chartio_host,config.chartio_db,config.chartio_id,config.chartio_pwd))
cur = con.cursor()

f = open("/home/schoi/scripts/NCMBC_prospect_without_app_list_3.20.14.sql",'r')
query = "".join(i for i in f.read() if ord(i)<128)
cur.execute(query)
data_prospect = pd.DataFrame(cur.fetchall(),columns=[desc[0] for desc in cur.description])

f2 = open("/home/schoi/scripts/NCMBC_prospect_approved_list_3.20.14.sql",'r')
query2 = "".join(i for i in f2.read() if ord(i)<128)
cur.execute(query2)
data_approved = pd.DataFrame(cur.fetchall(),columns=[desc[0] for desc in cur.description])

cur.close()
con.close()

writer = ExcelWriter('output.xlsx')
data_prospect.to_excel(writer,'prospects_without_app')
data_approved.to_excel(writer,'approved_prospects')
writer.save()

mail(config.recip,
   "Pangea Prospects List (NCMBC)",
   "Excel file contains prospects with no app and approved prospects in separate sheets",
   "output.xlsx")
