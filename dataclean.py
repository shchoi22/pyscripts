import pandas as pd
import numpy as np
import json
import urllib2
import psycopg2
from pandas.io import sql
from pandas import DataFrame
from datetime import datetime, date
import collections
import re

b_url = 'https://app.propertyware.com/pw/00a/195920567/JSON?2SajWeR'
u_url = 'https://app.propertyware.com/pw/00a/195920568/JSON?6FWtYyk'
l_url = 'https://app.propertyware.com/pw/00a/195920569/JSON?2TWZKZh'

wo_url = ['https://app.propertyware.com/pw/00a/196184161/JSON?6GfEUqC',
'https://app.propertyware.com/pw/00a/200638811/JSON?7KYzPuH',
'https://app.propertyware.com/pw/00a/200638812/JSON?6sDjBlf',
'https://app.propertyware.com/pw/00a/200638813/JSON?4TXcgTV',
'https://app.propertyware.com/pw/00a/200638814/JSON?4MrRDAy']

tc_url = 'https://app.propertyware.com/pw/00a/198410240/JSON?3vbdpeZ'
tr_url = 'https://app.propertyware.com/pw/00a/202277733/JSON?8oJhHEW'
dis_url = 'https://app.propertyware.com/pw/00a/202277734/JSON?9GStujS'

p_url = ['https://app.propertyware.com/pw/00a/200638805/JSON?5nykEpn',
'https://app.propertyware.com/pw/00a/202375723/JSON?1kkANiR',
'https://app.propertyware.com/pw/00a/202375724/JSON?1MesgHo',
'https://app.propertyware.com/pw/00a/202375725/JSON?4wZGZNg',
'https://app.propertyware.com/pw/00a/202375726/JSON?0InvzNJ']
cs_url = ['https://app.propertyware.com/pw/00a/198410241/JSON?3NNmZvY',
'https://app.propertyware.com/pw/00a/198901949/JSON?6qNwvOR',
'https://app.propertyware.com/pw/00a/199983104/JSON?2EoSOEB',
'https://app.propertyware.com/pw/00a/200114176/JSON?4ConJIk',
'https://app.propertyware.com/pw/00a/200114177/JSON?1DhjhNm',
'https://app.propertyware.com/pw/00a/202277732/JSON?3oIKPHW']

def buildingURL():
    return b_url

def prospectURL():
    return p_url

def unitURL():
    return u_url


def jsonToFrame (url):  #json data to dataframe
    request = urllib2.urlopen(url)
    data = request.read()
    data = data.replace("\\'","\'").replace(">","\>")
    frame = json.loads(data)
    
    columns = {}
    pattern = re.compile('[\W_]+')

    for items in frame['columns']:
        columns[items['index']]=items['label']
    for column in columns:
        columns[column] = pattern.sub('', columns[column])
        if columns[column][0].isdigit():
            columns[column] = '_' + columns[column]

    data_frame = pd.DataFrame(frame['records'], index=None).rename(columns=columns)
    return data_frame

def cleanData(data):
    return convertDate(convertNum(removeNonAscii(linkClean(data)))) 

def removeNonAscii(data):
    #pattern = re.compile('[\W_]+')
    #data = data.applymap(lambda x: pattern.sub(' ', x) if x is not None else x)
    data = data.applymap(lambda x: "".join(i for i in x if ord(i)<128) if isinstance(x,(str, unicode)) and x is not None else x)
    data = data.applymap(lambda x: x.replace('\n','').replace('\r','').replace(';','').replace('\\','') if isinstance(x,(str, unicode)) else None)
    return data

def linkClean(data):
    def stringClean(string):
        if isinstance(string,(str,unicode)) and string.find('<a name="') > -1:
            if len(string.lstrip('<a name="').split('" href')) > 1:
                return string.lstrip('<a name="').split('" href')[0]
            else:
                return ''
        else:
            return string
    return data.applymap(lambda x: stringClean(x))

def convertDate(data):
    def convertD(cdate):
        try:
            cdate = datetime.strptime(cdate,'%m/%d/%Y').date()
        except ValueError:
	    if cdate == "": 
               cdate = datetime.strptime('01/01/1900','%m/%d/%Y').date()
        return cdate
    for column in data.columns:
        if len(data[column].value_counts().index)>1 and isinstance(data[column].value_counts().index[1],(str,unicode)) and not any(c.isalpha() for c in data[column].value_counts().index[1]) and len(data[column].value_counts().index[1].split('/'))==3:
            data[column] = data[column].apply(lambda x: x if isinstance(convertD(x.split(' ')[0]),(str,unicode)) else convertD(x.split(' ')[0]))
#len(data[column].value_counts().index[1].split('/'))>=3
    return data

def convertNum(data):
    for column in data.columns:
        try:
            data[column] = data[column].astype(float)
        except:
            if isinstance(data[column].value_counts().index[0], unicode) and data[column].value_counts().index[0].find('$') > -1:
                data[column] = data[column].map(lambda x: x.replace('$','').replace(',',''))
                try:
                    data[column] = data[column].astype(float)
                except:
                    data[column] = data[column]
    return data

def mergeClean(frame):
    for column in frame.columns:
        if isinstance(frame[column].value_counts().index[0],date):
            frame[column] = frame[column].fillna(datetime.strptime('01/01/1900','%m/%d/%Y').date())
        elif isinstance(frame[column].value_counts().index[0],float) or isinstance(frame[column].value_counts().index[0],np.int64):
            frame[column] = frame[column].fillna(0.0)

    return frame

#Writes frame to postgresql database using copy from a temporary generated CSV
def writeFrame(con, report, frame):
    cur = con.cursor()
    cur.execute("TRUNCATE TABLE " + report)
    #con.commit()
    cur.execute("DROP TABLE IF EXISTS " + report)
    #cur.execute("VACUUM")
    #con.commit()
    cur.execute("CREATE TABLE " + report +'()')
    con.commit()
    
    dtype = ''
    for column in frame.columns:
        if isinstance(frame[column].value_counts().index[0],date):
            dtype = 'date'
        elif isinstance(frame[column].value_counts().index[0],float) or isinstance(frame[column].value_counts().index[0],np.int64):
            dtype ='numeric'
        else:
            dtype ='text'
        cur.execute("ALTER TABLE " + report +" ADD COLUMN " + column.lower() + " " + dtype+";")
        con.commit()
    frame.to_csv(report+'output.csv', sep=';', na_rep='', cols=None, header=False, index=False)   

    columnString = ','.join(frame.columns)
    cur.copy_from(open(report+'output.csv','r'), report, sep=';', null='NA', columns=None)
    con.commit()
  
    cur.execute("GRANT ALL ON TABLE " + report +" TO GROUP reporting_role;")
    con.commit()
    cur.close()

