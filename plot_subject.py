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
            new_frame = pd.read_csv(f,index_col=0,header=None)
            prefix = str(subject)+'_'
            new_frame.index = prefix+new_frame.index.astype(str)
            frames.append(new_frame)
        else:
            cluster_number_prefix = str(subject)+'_'+f.split('_')[-1].split('.')[0]+'_'
            new_frame = pd.read_csv(f,index_col=0,header=None)
            new_frame.index = cluster_number_prefix+new_frame.index.astype(str)        
            frames.append(new_frame)      
            print len(frames)
    allframe =  pd.concat(frames)
    centroids = set([item.split('_')[0] for item in allframe.index.astype(str) if '_' in item])
    return allframe
    #return allframe[~allframe[0].astype(str).isin(centroids)]
def get_top_500_sizes(df):
    '''
    supposed the df was fed by read_subject_sizes where
    1. index should be subject_primarycluster_secondarycluster_number
    2. index should be of type str

    '''
    return list(df.sort_values(1,ascending=False).head(500).index)
if __name__=='__main__':
    print 'subject number:', sys.argv[1]
    data = read_subject_sizes(subject = sys.argv[1])
    print get_top_500_sizes(data)
