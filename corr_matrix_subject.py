#running command: python corr_matrix_subject.py 0 cluster_centers/ max_point_distance/
import numpy as np
import sys,os,pandas,re
import pandas as pd
from os.path import join,getsize
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
from sklearn import metrics
from plot_subject import read_subject_sizes as read_size
from plot_subject import get_top_500_sizes as get_top
import copy
def save_matrix_png(data,filename):
    plt.cla()
    plt.matshow(data)
    plt.colorbar()
    plt.savefig(filename)

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
        template = '.+\d+_\d+\..+'
    pattern = re.compile(template)
    if pattern.match(file_path):
        return True
    else:
        return False
def get_cluster_name(file_path):
    '''
    we say main# is the main cluster number,
    we also say sub# is the sub cluster number, 
    returns: 
    main#_sub#_0-49 if sub# is present. 
    main#_0=499 if no sub#
    '''
    if is_sub_cluster(file_path):
        main_num = main_number(file_path)
        sub_nums = sub_numbers(file_path)
        return [sub_nums+'_'+str(item) for item in range(50)]
    else:
        main_num = main_number(file_path)
        return [main_num +'_'+str(item) for item in range(500)]

def sub_numbers(file_path):
    '''
    e.g.cluster_centers/cluster_centers_subject0_40.csv
    should return 0_40
    getting the cluster number
    Args:
        file_path: the path of file
    Returns: 
        String of cluster number if sub cluster
        None if not
    '''
    template = '([^0-9]*)(\d+_\d+)(.+)'
    pattern = re.compile(template)
    m = pattern.match(file_path)
    if m:
        cluster_number = m.groups()[-2]
        return cluster_number
    else:
        return None
def main_number(file_path):
    '''
    e.g. cluster_centers/cluster_centers_subject0_40.csv
    should return 0
    Args:
        file_path: the path of file
    Returns:
        String of main cluster number if sub cluster
        None if not
    '''
    template = '([^0-9]*)(\d+)(_.+)'
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

def read_files_center(file_paths):
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
def read_files_max(file_paths):
    '''
    Args:
        file_paths: a list of string of file path. 
    Return:
        a pandas dataframe of all file_paths
    '''
    dfs = []
    for file_path in file_paths:
        data = pd.read_csv(file_path,header=None,index_col=0)
        if is_sub_cluster(file_path):
            prefix = sub_numbers(file_path)+'_' 
        else:
            prefix = main_number(file_path)+'_'
        data.index = prefix+data.index.astype(str)
        dfs.append(data)
    return pd.concat(dfs)

if __name__=='__main__':
    if len(sys.argv)<4:
        print 'ERROR: no subject number supplied'
        print 'Usage: python corr_matrix_subject.py subject# cluster_center_path max_dis_path'
        print 'e.g. python corr_matrix_subject.py 0 cluster_centers/ max_point_distance/'
        sys.exit(1)
    subject = sys.argv[1]
    path = sys.argv[2]
   
   #print 'getting correlation matrix for subject',subject
    template = 'cluster_centers_subject'+str(subject)+'_.*csv'
    file_names = get_files(path,template)
    data = np.array(read_files_center(file_names))
    
    #obtain 1000*1000 cluster
    top_list = get_top(read_size(subject = subject))
    top_list = list(top_list)
    cluster_names = []
    for file_name in file_names:
        cluster_names.extend(get_cluster_name(file_name))
    template_max = str(subject)+'_.*csv'
    file_names_max = get_files(sys.argv[3],template_max)
    data_max = read_files_max(file_names_max)
    #result for top 500 clusters
    r500 = [[0]*500]*500
    r500_2 = [[0]*500]*500
    count1 = 0
    count2 = 0
    count3 = 0
    for i in xrange(len(r500)):
        for j in xrange(len(r500)):
            #first get the pairwise enclidean distance
            name_i = top_list[i]
            name_j = top_list[j]
            data_i = cluster_names.index(name_i)
            data_j = cluster_names.index(name_j)
            r500[i][j] = float(np.sqrt(np.sum((data[data_i]-data[data_j])**2))+data_max.ix[name_i]+data_max.ix[name_j])
            r500_2[i][j] = float(np.sqrt(np.sum((data[data_i]-data[data_j])**2))-data_max.ix[name_i]-data_max.ix[name_j])
            if r500_2[i][j]>0.000001:
                count1 += 1 
            #count of values on the right that are smaller than 1.34.
            if r500[i][j]<1.34:
                count2 += 1
            #the count of values on the left that are larger than 1.48.
            if r500_2[i][j]>1.48:
                count3 += 1
    print subject,',',count1,',',count2,',',count3
    save_matrix_png(r500,'pluses'+str(subject)+'.png')
    save_matrix_png(r500_2,'minuses'+str(subject)+'.png')
    
    '''
    result = metrics.pairwise.pairwise_distances(data)
    result2 = copy.copy(result)
    count1 = 0
    count2 = 0
    count3 = 0
    cluster_names = list(cluster_names)
    
    #show results for top 500 clusters

    #print 'original cluster names',len(cluster_names)
    #print 'top list length',len(top_list)
    #print 'cluster names - top list',len(set(cluster_names)-set(top_list))
    for i in range(len(result)):
        for j in range(len(result[0])):
            result[i][j]+=data_max.ix[cluster_names[i]]
            result2[i][j]-=data_max.ix[cluster_names[i]]
            result[i][j]+=data_max.ix[cluster_names[j]]
            result2[i][j]-=data_max.ix[cluster_names[j]]
            if cluster_names[i] in top_list and cluster_names[j] in top_list:
                #print 'both i and j are in top list',i,j
                #count of values on the left that are larger than 0.000001
                if result2[i][j]>0.000001:
                    count1 +=1 
                #count of values on the right that are smaller than 1.34.
                if result[i][j]<1.34:
                    count2 +=1
                #the count of values on the left that are larger than 1.48.
                if result2[i][j]>1.48:
                    count3+=1
            else:
                result[i][j] = 0
                result2[i][j]=2
    print subject,',',count1,',',count2,',',count3
    plt.matshow(result)
    plt.colorbar()
    plt.savefig('pluses'+str(subject)+'.png')

    plt.cla()
    plt.matshow(result2)
    plt.colorbar()
    plt.savefig('minuses'+str(subject)+'.png')
    '''
