from matplotlib import pylab as plt
import sys
import pandas as pd
from glob import glob
subject = 7
def read_subject_sizes(subject = subject):
    file_names = glob('cluster_sizes_subject'+str(subject)+'*.csv')
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
    return pd.concat(frames)

if __name__=='__main__':
    print 'subject number:', sys.argv[1]
