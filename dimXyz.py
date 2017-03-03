from io_routines import readMat2
#from db_utilities import prepareInsert,prepareCreateTable,getSession
import numpy as np
import sys,time,os,threading

def dft_worker(f,data,subject,file_name):
    '''
    Args: 
        f: the real data, from which you need references to extract data using "data"
        data: reference data, in 5 dimensions: s,t,x,y,z
            where s subject, t time, x,y,z are 3 dimensional points
        subject: integer
        file_name: a string that we want to write in. e.g "subject0.dat"
    Returns: 
        None. 
    File: 
        normalized DFT output for subjects. 
    '''
    print 'dft_worker for',subject,'started',time.time()
    newfile = open(file_name,'a')
    data2 = np.array(f[data[subject][0]])
    start_time2 = time.time();
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

    for z in xrange(len(data2[0][0][0])):
        for y in xrange(len(data2[0][0])):
            for x in xrange(len(data2[0])):
                timeSeries = [data2[t][x][y][z] for t in xrange(len(data2))]
                timeSeries = np.array(timeSeries).astype(float)
                #timeSeries = dft_y(timeSeries)
                xyzKey = str(x)+'|'+str(y)+'|'+str(z)
                line=','.join([xyzKey,str(x),str(y),str(z)])
                #line = ','.join([str(x),str(y),str(z),str(subject),','.join([str(item) for item in timeSeries])])
                newfile.write(line+'\n')
    newfile.close()
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))


def main():
    start_time = time.time();

    fileNames = ['HitchcockData.mat']
    for fileName in fileNames:
        f,data = readMat2(fileName)
    threads = []

    for subject in xrange(1):
        newfileName = 'dimXyz.csv'
        t = threading.Thread(target=dft_worker, args=(f,data,subject,newfileName))
        threads.append(t)
        t.start()
    print("--- total run time: %s seconds ---" % str(time.time() - start_time))


if __name__ == '__main__':
    main()
