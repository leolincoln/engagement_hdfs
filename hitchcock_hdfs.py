# -*- coding: utf-8 -*-
'''
Created on Sun Nov 09 14:11:45 2014

@author: liu
'''

from io_routines import readMat2
#from db_utilities import prepareInsert,prepareCreateTable,getSession
import numpy as np
import sys,time,os,threading
def norm_nodevide(x):
    '''
    1. normalize the series in numpy array float64 format
    2. taking fft of the series numpy array float 64 format
    3. return the transformed data numpy array float 64 format
    Args: 
        x float64 numpy array
    Returns:
        result float64 numpy array
    '''
    result = []
    x = np.array(x).astype(float)
    mean = x.mean()
    d = 0
    for s in x:
        d+=(s-mean)**2
    d = d**0.5
    if d==0:
        print 'Encountered 0 nominator on series'
        return x
    for s in x:
        result.append((s-mean)/d)
    
    return result
    
def dft_y(x):
    '''
    Args:
        x, array. the input time series
    Returns:
        y, array. the output dft time series
    '''
    x = np.array(x).astype(float)
    result = np.fft.fft(x)
    return np.abs(result)

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
                timeSeries = norm_nodevide(timeSeries)
                #timeSeries = dft_y(timeSeries)
                line = ';'.join([str(x),str(y),str(z),','.join([str(item) for item in timeSeries])])
                newfile.write(line+'\n')
    newfile.close()
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))

def dft_worker2(f,datas,x,y,z,file_name=None,data_folder = None):
    '''
    Args: 
        f: the real data, from which you need references to extract data using "data"
        data2: reference datas, array of data in 5 dimensions: s,t,x,y,z here we will have 22 data. 
            where s subject, t time, x,y,z are 3 dimensional points
        x: integer of x
        y: integer of y
        z: integer of z
        file_name: a string that we want to write in. e.g "1_1_1.dat" 
    Returns: 
        None. 
    File: 
        normalized DFT output for subjects. 
    '''
    if file_name is None:
        file_name = str(x)+'_'+str(y)+'_'+str(z)+'.dat'
    if data_folder is None:
        data_folder = './'
    else if data_folder[-1]!='/':
        data_folder+='/'
    print 'dft_worker for',x,y,z,'started',time.time()
    try:
        os.remove(data_folder+file_name)
    except:
        print 'no file found. Program continues '+data_folder+file_name 
    newfile = open(data_folder+file_name,'a')
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
    22 subjects for hitchcockdata
    '''

                timeSeries = [data2[t][x][y][z] for t in xrange(len(data2))]
                timeSeries = np.array(timeSeries).astype(float)
                timeSeries = norm_nodevide(timeSeries)
                #timeSeries = dft_y(timeSeries)
                line = ';'.join([str(x),str(y),str(z),','.join([str(item) for item in timeSeries])])
                newfile.write(line+'\n')
    newfile.close()
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))
def main():
    start_time = time.time();

    fileNames = ['HitchcockData.mat']
    for fileName in fileNames:
        try:
            f,data = readMat2(fileName)
        except:
            sys.exit('not found file '+fileName+':  exiting')
    threads = []

    for subject in xrange(len(data)):
        newfileName = fileName[:-4]+str(subject)+'.dat'
        t = threading.Thread(target=dft_worker, args=(f,data,subject,newfileName))
        threads.append(t)
        t.start()
    print("--- total run time: %s seconds ---" % str(time.time() - start_time))

def main2():
    '''
    Purpose of this main: 
    Hoepfully I will be focused on building a dictinoary of standardized data. 
    The dictionary should have xyz as keys, 
    and list of time series as its values, 
    with position 0 referring to the 0th subject from mri data
    and position n referring to teh nth subject from mri data. 
    for each subject, the time series for each xyz shoud be normalized so as not to create unblaanced scaling for different subjects. 
    Reduce time by this structure? I think not. 
    Now that I think about it. I should have used the old format. Garenteed to work lol. Okay got it. 
    '''
    data_folder = 'data/'
    start_time = time.time()
    fileNames = ['HitchcockData.mat']
    for fileName in fileNames:
        try:
            f,data = readMat2(fileName)
        except:
            sys.exit('not found file '+fileName+' ;exiting')
    #f[data[subject][0]] gives the actual subject data. 
    #this wierld structure is what saved memory i guess
    datas = []
    for subject in xrange(len(data)):
        datas.append(f[data[subject][0]]
    data2 = datas[0]
    threads = []
    for x in xrange(len(data2[0])):
        for y in xrange(len(data2[0][0]):
            for z in xrange(len(data2[0][0][0]):
                t = threading.Thread(target=dft_worker2,args=(f,datas,x,y,z,data_folder))
                threads.append(t)
                t.start()
    print("--- total run time: %s seconds ---" % str(time.time() - start_time))
