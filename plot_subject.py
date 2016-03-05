from matplotlib import pylab as plt
import sys
import pandas as pd
from glob import glob
subject = 7
import fnmatch
import os
#find_match('src','*.csv')
def find_match(path,pattern):
    matches = []
    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))
    return matches

def read_subject_sizes(subject = subject):
    file_names = find_match('cluster_sizes','cluster_sizes_subject'+str(subject)+'*.csv')
    frames = []
    for f in file_names:
        print 'processing',f
        if f.split('_')[-1]=='subject'+str(subject)+'.csv':
            frames.append(pd.read_csv(f,header=None))
        else:
            cluster_number_prefix = f.split('_')[-1].split('.')[0]+'_'
            new_frame = pd.read_csv(f,header=None)
            new_frame[0] = cluster_number_prefix+new_frame[0].astype(str)        
            frames.append(new_frame)      
            print len(frames)
    allframe =  pd.concat(frames)
    centroids = set([item.split('_')[0] for item in allframe[0].astype(str) if '_' in item])
    return allframe[~allframe[0].astype(str).isin(centroids)]

if __name__=='__main__':
    print 'subject number:', sys.argv[1]
    data = read_subject_sizes(subject = sys.argv[1])
