# -*- coding: utf-8 -*-
'''
Created on Sun Nov 09 14:11:45 2014

@author: liu
'''

from io_routines import readMat2
from db_utilities import prepareInsert,prepareCreateTable,getSession
import numpy as np
import sys
import time
#use cpickle instead of pickle to speed up the process.
#import cPickle as pickle
#Recording time.
start_time = time.time();

fileNames = ['PiemanData.mat','HitchcockData.mat']
#fileNames = ['HitchcockData.mat']
'''
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['10.10.10.10'],auth_provider=auth_provider, protocol_version=2)
session = cluster.connect('engagement')
#session.set_keyspace('users')
# or you can do this instead
#session.execute('USE users')
session.execute()
cluster.shutdown()
'''
#subjects store the range of subject.
subjects=range(22)
def tryInsert(session,s,time):
    try:
        session.execute(s)
    except:
        print('failed to insert, retrying: '+s[0:100]+' '+str(time))
        tryInsert(session,s,time+1)

#getting the cassandra db session
try:
    session = getSession(username = 'cassandra',password='cassandra')
    session.default_timeout = 30  # this is in *seconds* setting the default timeout will prevent the exception.
except:
    print 'not able to start db session'

tempCreate = prepareCreateTable("pieman_corr_new",valueNames=['id1','id2','corr'],valueTypes=['text','text','double'],primaryKeys=['id1','id2'],comment='corr_results')
tryInsert(session,tempCreate,1)
'''



#processing the data.
tempInsert = ''
for fileName in fileNames:
    for subject in subjects:
        tempCreate = prepareCreateTable(fileName[:-4]+'corrResults'+str(subject),valueNames=['id1','id2','corr'],valueTypes=['text','text','double'],primaryKeys=['id1','id2'],comment='corr_results')
        print('creating table:' + tempCreate)
        tryInsert(session,tempCreate,1)
for fileName in fileNames:
    tempCreate = prepareCreateTable(fileName[:-4]+'normalTest',valueNames=['id1','id2','normality'],valueTypes=['text','text','boolean'],primaryKeys=['id1','id2'],comment='normal_test_results')
    print('creating table:' + tempCreate)
    tryInsert(session,tempCreate,1)

'''

#after processing, shutdown the session.
session.shutdown()
print("--- total run time: %s seconds ---" % str(time.time() - start_time))
