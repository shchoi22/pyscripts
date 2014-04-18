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
    #return convertDate(removeNonAscii(linkClean(data)))

def removeNonAscii(data):
    data = data.applymap(lambda x: "".join(i for i in x if ord(i)<128) if isinstance(x,(str, unicode)) and x is not None else x)
    data = data.applymap(lambda x: x.replace('\n','').replace('\r','').replace(';','').replace('\\','') if isinstance(x,(str, unicode)) else x)
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
        if not isinstance(data[column].value_counts().index[0],int):
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
    
    dtypes = ''
    for column in frame.columns:
        if isinstance(frame[column].value_counts().index[0],date):
            dtype = 'date'
        elif isinstance(frame[column].value_counts().index[0],int): 
            dtype = 'integer'
        elif isinstance(frame[column].value_counts().index[0],float):
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

