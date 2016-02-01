# -*- coding: utf-8 -*-
"""
Created on Mon Oct 06 09:10:15 2014

@author: liu
"""

import scipy.io
import numpy as np

path = './piemanData.mat'
#for matlab version lower than 7.3 
def readMat(path):
    # for lowerthan 7.3 version mat files. 
    # path contains the path of the mat file. 
    mat = scipy.io.loadmat(path)
    s = mat['s'][0]
    subjects = mat['subjects'][0]
    return s, subjects

def readMat2(path):
    ''' for v7.3 or above mat files. 
    ''' 
    import numpy as np, h5py 
    f = h5py.File(path,'r')
    data = f['s']
    data = np.array(data) # For converting to numpy array
    return f,data

#pickle.dump( data, open(fileName,'wb') )