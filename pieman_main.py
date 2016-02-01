# -*- coding: utf-8 -*-
from io_routines import *
from db_utilities import *
import sys
import time
import traceback
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
        from time import sleep
        sleep(0.1)
    except Exception as e:
        print('failed to insert,retrying: '+str(time))
        print e
        from time import sleep
        sleep(0.1)
        tryInsert(session,s,time+1)

for fileName in fileNames:
    try:
        data, subjects = readMat(fileName)
    except:
        sys.exit('not found file '+fileName+':  exiting')
try:
    session = getSession(username = 'leolincoln',password='ll7713689')
except Exception as e:
    print e
    print 'not able to start session'
#for all datas: 
tempInsert = ''

for subject in xrange(len(data)):
    tempCreate = prepareCreateTable(fileName[:-4]+str(subject),valueNames= ['x','y','z','data'],valueTypes = ['int','int','int','list<int>'],primaryKeys=['x','y','z'],comment='x_y_z_listdata')
    print('creating table'+tempCreate)
    tryInsert(session,tempCreate,1)

    print 'subject #:',subject
    tempData = data[subject]
    batchInsert=[]
    '''
    When you read pieman.mat from matlab, you will get a 58x40x46x274 data.
    58 --> Z. from lower to top
    40 --> Y, from front to back
    46 --> X, from left to right.
    274 --> time, from lower time to higher time
    '''    

    for z in xrange(len(data[0])):
        startTime2 = time.time()
        for y in xrange(len(data[0][0])):
            batchInsert=[]
            batchInsert.append('BEGIN BATCH')
            for x in xrange(len(data[0][0][0])):
                #print 999
                #the data for subject is in data[i][x][y][z][time]
                #print str(x),str(y),str(z)
                tempId = "'"+str(subject)+"|"+str(x)+"|"+str(y)+"|"+str(z)+"'"
                print('current: '+tempId)
                tempInsert = prepareInsert('engagement',fileName[:-4]+str(subject),[x,y,z,data[subject][z][y][x]],tableColumns = '(x,y,z,data)')
                #print tempInsert
                batchInsert.append(tempInsert)
                #session.execute(tempInsert)
                #print 'inserting: '+ tempInsert
            batchInsert.append(' APPLY BATCH;')
            temp = ' '.join(batchInsert)
            tryInsert(session,temp,1)
            print 'insert at z,y',z,y,'  finished: ',time.time()-startTime2
session.shutdown()

print("--- run time: %s seconds ---" % str(time.time() - start_time))
