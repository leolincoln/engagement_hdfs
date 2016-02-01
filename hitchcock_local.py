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

def subject_worker(f,data,subject,d,lock):
    '''
    Args: 
        f: the real data, from which you need references to extract data using "data"
        data: reference data, in 5 dimensions: s,t,x,y,z
            where s subject, t time, x,y,z are 3 dimensional points
        subject: integer
        d: the global dictionary that we want to write to
        lock: a RLock object from threading, incase the x,y,z pair has not been initialized
    Returns: 
        None. 
    File: 
        normalized DFT output for subjects. 
    '''
    print 'dft_worker for',subject,'started',time.time()
    
    #new file begins, commenting out because we will not use it
    #newfile = open(file_name,'a')
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
                xyz_key = str(x)+'_'+str(y)+'_'+str(z)
                if xyz_key not in d.keys():
                    lock.acquire()
                    try:
                        d[xyz_key]={}
                    finally:
                        lock.release()
                #now the lock is released, and we should have xyz_key in our global d dictionary
                timeSeries = [data2[t][x][y][z] for t in xrange(len(data2))]
                timeSeries = np.array(timeSeries).astype(float)
                timeSeries = norm_nodevide(timeSeries)
                d[xyz_key][subject] = timeSeries
                #comment out the dft transform code as we will not use it
                #timeSeries = dft_y(timeSeries)
                
                #comment out the line code as we will not be writing to subject file directly
                #line = ';'.join([str(x),str(y),str(z),','.join([str(item) for item in timeSeries])])
                #newfile.write(line+'\n')
    #new file ends. Commenting out because we will not use it
    #newfile.close()
    print("--- run time for subject: %s seconds ---" % str(time.time() - start_time2))


def main():
    start_time = time.time();
    lock = threading.RLock()
    fileNames = ['HitchcockData.mat']
    for fileName in fileNames:
        try:
            f,data = readMat2(fileName)
        except:
            sys.exit('not found file '+fileName+':  exiting')
    threads = []
    #this result_dict should be a giant dictionary of the form
    #key -- x,y,z
    #value -- dictionary2 
    #dictionary2:
    #key: subject number
    #value: array of time series
    #Warning: This is a expoitation of the GIL global interpreter lock. If you are not using cython then problem might occur.
    #the only lock that I deployed here are at the time when creating a new dictionary2 inside the original dictionary
    result_dict = {}
    for subject in xrange(len(data)):
        newfileName = fileName[:-4]+str(subject)+'.dat'
        t = threading.Thread(target=dft_worker, args=(f,data,subject,newfileName,lock))
        threads.append(t)
        t.start()
    print("--- total run time: %s seconds ---" % str(time.time() - start_time))

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
