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
tempInsert = ''
for fileName in fileNames:
    data,subjects = readMat(fileName)
for subject in xrange(len(data)):
    newfileName = fileName[:-4]+str(subject)
    f = open(newfileName,'a')
    print 'subject #:',subject
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
                tempId = "'"+str(subject)+"|"+str(x)+"|"+str(y)+"|"+str(z)+"'"
                print('current: '+tempId)
                line = ';'.join([str(x),str(y),str(z),','.join([str(item) for item in data[subject][z][y][x]])])
                f.write(line+'\n')
            print 'write line at z,y',z,y,'  finished: ',time.time()-startTime2
    f.close() 
print("--- run time: %s seconds ---" % str(time.time() - start_time))
