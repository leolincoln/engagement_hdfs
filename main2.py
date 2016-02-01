# -*- coding: utf-8 -*-
"""
Created on Sat Nov  8 13:21:49 2014

@author: leoliu
"""

from io_routines import readMat
from db_utilities import prepareInsert,prepareCreateTable,getSession
import sys
import time
#use cpickle instead of pickle to speed up the process. 
#import cPickle as pickle
#Recording time. 
start_time = time.time();

#fileNames = ['PiemanData.mat','HitchcockData.mat']
fileNames = ['PiemanData.mat']
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
        print 'failed to insert, retrying: ',time
        tryInsert(session,s,time+1)

#reading data from file specified in fileNames array. 
for fileName in fileNames:
    try:
        data, subjects = readMat(fileName)
    except:
        sys.exit('not found file '+fileName+':  exiting')

try:
    session = getSession(username = 'leolincoln',password='ll7713689')
except:
    print 'not able to start session'


#processing the data. 
tempInsert = ''
for subject in range(len(data)):
    start_time2 = time.time();
    batchInsert = []
    tempData = data[subject]
    tempCreate = prepareCreateTable(fileName[:-4]+'t'+str(subject))
    session.execute(tempCreate)
    '''
    When you read pieman.mat from matlab, you will get a 58x40x46x274 data.
    58 --> Z. from lower to top
    40 --> Y, from front to back
    46 --> X, from left to right.
    274 --> time, from lower time to higher time
    '''    
    
    for t in xrange(len(data[0][0][0][0])):
        start_time3 = time.time();
        batchInsert=[]
        batchInsert.append('BEGIN BATCH')
        print 'at time: ',t
        for z in xrange(len(data[0])):
            for y in xrange(len(data[0][0])):
                for x in xrange(len(data[0][0][0])):
                    #print 999
                    #the data for subject is in data[i][x][y][z][time]
                    #print str(x),str(y),str(z),str(t)
                    tempInsert = prepareInsert('engagement',fileName[:-4]+'t'+str(subject),[x,y,z,t,data[subject][z][y][x][t]],tableColumns = ' (x,y,z,time,data) ')                    
                                 
                    #print tempInsert;
                    batchInsert.append(tempInsert)
                    '''                    
                    if(len(batchInsert)>10):
                        batchInsert.append(' APPLY BATCH;')
                        sys.exit(0)
                    '''
                    #session.execute(tempInsert)
                    #print 'inserting: '+ tempInsert 
        batchInsert.append(' APPLY BATCH;')
        temp = ' '.join(batchInsert)
        tryInsert(session,temp,1)
      #  session.execute(temp)
        print("--- run time for t: %s seconds ---" % str(time.time() - start_time3))
        
       
        
    #pickle.dump( ' '.join(batchInsert), open('subject'+str(subject),'wb') )
    print 'subject:',subject
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))
    
#after processing, shutdown the session. 
session.shutdown()
print("--- total run time: %s seconds ---" % str(time.time() - start_time))
