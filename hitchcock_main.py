# -*- coding: utf-8 -*-
"""
Created on Sun Nov 09 14:11:45 2014

@author: liu
"""

from io_routines import readMat2
from db_utilities import prepareInsert,prepareCreateTable,getSession
import numpy as np
import sys
import time
#use cpickle instead of pickle to speed up the process. 
#import cPickle as pickle
#Recording time. 
start_time = time.time();

#fileNames = ['PiemanData.mat','HitchcockData.mat']
fileNames = ['HitchcockData.mat']
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

def tryInsert(session,s,time):
    try:
        session.execute(s)
    except:
        print('failed to insert, retrying: '+s[0:100]+' '+str(time))
        tryInsert(session,s,time+1)

#reading data from file specified in fileNames array. 
for fileName in fileNames:
    try:
        f,data = readMat2(fileName)
    except:
        sys.exit('not found file '+fileName+':  exiting')

try:
    session = getSession(username = 'leolincoln',password='ll7713689')
    session.default_timeout = 30  # this is in *seconds* setting the default timeout will prevent the exception. 
except:
    print 'not able to start db session'





#processing the data. 
tempInsert = ''
tempCreate = prepareCreateTable(fileName[:-4]+'total')
print('creating table:' + tempCreate)
tryInsert(session,tempCreate,1)
for subject in xrange(len(data)):
    data2 = np.array(f[data[subject][0]])
    start_time2 = time.time();
    batchInsert = []
    tempData = data[subject]
    
    
    '''
    When you read pieman.mat from matlab, you will get a 58x40x46x274 datc.

    58 --> Z. from lower to top
    40 --> Y, from front to back
    46 --> X, from left to right.
    274 --> time, from lower time to higher time
    for hitchcockdatat*, you will get 601 x 47 x 41 x 59 data references. 
    time -- x -- y -- z
    22 subjects for hitchcockdatao

    '''   
    
    for t in xrange(len(data2)):
        start_time3 = time.time();
        batchInsert=[]
        batchInsert.append('BEGIN BATCH')
       #print 'at time: ',t
        for z in xrange(len(data2[0][0][0])):
            for y in xrange(len(data2[0][0])):
                for x in xrange(len(data2[0])):
                    #print 999
                    #the data for subject is in data[i][x][y][z][time]
                    #print str(x),str(y),str(z),str(t)
                    tempInsert = prepareInsert('engagement',fileName[:-4]+'total',[subject,x,y,z,t,data2[t][x][y][z]],tableColumns = ' (subject,x,y,z,time,data) ')                    
                                 
                    #print tempInsert;
                    batchInsert.append(tempInsert)
                    #dont forget to comment this line out when in actual enviornment                    
                    #sys.exit(0)
                    '''                  
                    if(len(batchInsert)>10):
                        batchInsert.append(' APPLY BATCH;')
                        sys.exit(0)
                    '''
                    #session.execute(tempInsert)
                    #print 'inserting: '+ tempInsert 
        batchInsert.append(' APPLY BATCH;')
        temp = ' '.join(batchInsert)
        #sys.exit(0)
        #use the newly defined session timeouts. 
        #session.execute(temp)
        tryInsert(session,temp,1)
      #  session.execute(temp)
        print("--- run time for t: %s seconds ---" % str(time.time() - start_time3))

    #pickle.dump( ' '.join(batchInsert), open('subject'+str(subject),'wb') )
    print 'subject:',subject
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))
    
#after processing, shutdown the session. 
session.shutdown()
print("--- total run time: %s seconds ---" % str(time.time() - start_time))
