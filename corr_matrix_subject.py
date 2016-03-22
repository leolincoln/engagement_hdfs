import sys,os,pandas,re
import pandas as pd
from os.path import join,getsize
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
from sklearn import metrics
from plot_subject import read_subject_sizes
def get_files(path,template):
    '''
        Args:
            path: the path to find files in
            template: the template to match names
        Returns: the list of files matching template in path
    will combile tempalte into pattern. 
    
    '''
    matches = []
    pattern = re.compile(template)
    for root, dirs,filenames in os.walk(path):
        for name in filter(lambda name:pattern.match(name),filenames):
            matches.append(join(root,name))
    return matches

def is_sub_cluster(file_path,template = None):
    if template is None:
        template = 'cluster_centers_subject\d+_\d+.+'
    pattern = re.compile(template)
    if pattern.match(file_path):
        return True
    else:
        return False

def file_path2cluster_number(file_path):
    '''
    e.g.cluster_centers/cluster_centers_subject0_40.csv
    should return 40
    getting the cluster number
    Args:
        file_path: the path of file
    Returns: 
        String of cluster number if sub cluster
        None if not
    '''
    file_path = os.path.basename(file_path)
    if is_sub_cluster(file_path):
        return file_path.split('.')[-2].split('_')[-1]
    else:
        return None

def file_path2cluster_main_number(file_path):
    '''
    e.g. cluster_centers/cluster_centers_subject0_40.csv
    should return 0
    Args:
        file_path: the path of file
    Returns:
        String of main cluster number if sub cluster
        None if not
    '''
    template = '(.+)(subject)(\d+)(.+)'
    pattern = re.compile(template)
    m = pattern.match(file_path)
    if m:
        cluster_number = m.groups()[-2]
        return cluster_number
    else:
        return None


def read_file(file_path):
    '''
    read in a file and return a pandas dataframe. 
    Args:
        file_path: the string of file path
    '''
    data = pd.read_csv(file_path,header=None)
    return data

def read_files(file_paths):
    '''
    Args:
        file_paths: a list of string of file path. 
    Return:
        a pandas dataframe of all file_paths
    '''
    dfs = []
    for file_path in file_paths:
        data = read_file(file_path)
        #remove the last None because I put an extra ',' at the end of each line
        data = data[data.columns[:-1]]
        dfs.append(data)
    return pd.concat(dfs)

if __name__=='__main__':
    if len(sys.argv)<3:
        print 'ERROR: no subject number supplied'
        print 'Usage: python corr_matrix_subject.py subject# cluster_center_path'
        print 'e.g. python corr_matrix_subject.py 0 cluster_centers/'
        sys.exit(1)
    subject = sys.argv[1]
    path = sys.argv[2]
    print 'getting correlation matrix for subject',subject
    template = 'cluster_centers_subject'+str(subject)+'.*csv'
    file_names = get_files(path,template)
    index_clusternum,data = read_files(file_names) 
    #obtain 1000*1000 cluster
    result = metrics.pairwise.pairwise_distances(data)
    sizes = read_subject_sizes(subject=subject)
    #plt.matshow(result)
    #plt.colorbar()
    #plt.savefig('test'+str(subject)+'.png')
