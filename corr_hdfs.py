from pyspark import SparkContext
import os,itertools,time,math
import numpy as np
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt
import pickle
def xyz_feature(xyz_value):
    xyz_key = xyz_value[0]
    xyz_dict = xyz_value[1]
    features = [np.array(xyz_dict[key].split(',')).astype(float) for key in sorted(xyz_dict)]
    merged = list(itertools.chain.from_iterable(features))
    return (xyz_key,merged) 

def value_pairs(line):
    '''
    get 'x,y,z' => values key=> value pairs
    Args:
        line is string of a line
        x;y;z;t0,t1,t2,...,tn
    '''
    values = line.split(';')
    #so now the first 3 are x,y and z.
    x=  values[0]
    y = values[1]
    z = values[2]
    subject = values[3]
    timeseries = values[4]
    return ((x,y,z),{subject:timeseries})
def xyz_group(xyz1,xyz2):
    full = xyz1
    full.update(xyz2)
    return full

def print_rdd(rdd):
    for x in rdd.collect():
        print x
def count_rdd(rdd):
    count = 0
    for x in rdd.collect():
        count+=1
    return count

def get_eud(values):
    pair1 = values[0]
    pair2 = values[1]
    xyz1 = pair1[0]
    xyz2 = pair2[0]
    v1 = [float(item) for item in pair1[1].split(',')]
    v2 = [float(item) for item in pair2[1].split(',')]
    result = sum([(v1[i]-v2[i])**2 for i in range(len(v1))])
    return ((xyz1,xyz2),math.sqrt(result))
def filter_0(line):
    array = [float(item) for item in line.split(';')[3]]
    return sum(array)!=0

time_now = time.time()
sc = SparkContext()
hdfsPrefix = 'hdfs://wolf.iems.northwestern.edu/user/huser54/'
fileName1 = 'engagement/'
fileName2 = 'engagementsample/'
lines = sc.textFile(hdfsPrefix+fileName1)
#map the values to xyz string -> dictionary of subjects with time series. 
values = lines.map(value_pairs)
print 'values obtained'
#print values.first()
#print 'value obtain time:',time.time()-time_now
time_old = time.time()

#group by key. Using reduce. Because groupby is not recommended in spark documentation
groups = values.reduceByKey(xyz_group)
print 'groups finished'
#print groups.first()
#print 'group obtain time:',time.time()-time_now
time_now = time.time()

#map the groups to xyz -> array, where array is 0-22 subject points. 
feature_groups = groups.map(xyz_feature)
print 'feature group'
#print feature_groups.first()
#print 'feature obtain time:',time.time()-time_now
time_now = time.time()

parsedData = feature_groups.map(lambda x:x[1])
print 'parsed data'
#print parsedData.first()
#print 'parsed data obtain time:',time.time()-time_now
time_now = time.time()
#now we have xyz -> group of features
#and we are ready to cluster. 
# Build the model (cluster the data)
#document states:
#classmethod train(rdd, k, maxIterations=100, runs=1, initializationMode='k-means||', seed=None, initializationSteps=5, epsilon=0.0001,initialModel=None)
clusters = KMeans.train(parsedData, 700, maxIterations=100,runs=10, initializationMode="k-means||")
print 'cluster obtain time:',time.time()-time_now
time_now = time.time()

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
with open('WSSE.dat','w') as f:
    f.write(str(WSSSE))

time_now = time.time()

#cluter centers after calculating kmeans clustering
#clusterCenters = sc.parallelize(clusters.clusterCenters)

print 'clearing hdfs system'
os.system('hdfs dfs -rm -r -f '+hdfsPrefix+'clusterCenters')
cluster_ind = parsedData.map(lambda point:clusters.predict(point))
cluster_ind.collect()
cluster_sizes = cluster_ind.countByValue().items()
#remove cluster size and center data
os.system('rm -rf cluster_sizes.dat')
os.system('rm -rf cluster_centers.dat')
pickle.dump(list(cluster_sizes),open('cluster_sizes.dat','w'))
pickle.dump(clusters.centers,open('cluster_centers.dat','w'))
#save as text file to clusterCenters in hdfs
print 'save cluster center',time.time()-time_now

print 'wssse obtain time:',time.time()-time_old
print("Within Set Sum of Squared Error = " + str(WSSSE))
'''
cart_value_pairs = values.cartesian(values)
cart_value_pairs.repartition(100)
print 'pairs cartesian called'
print cart_value_pairs.first()
corr_result = cart_value_pairs.map(get_eud)
print 'getting euclidean distance'
print corr_result.first()
corr_result_filter = corr_result.filter(lambda values2:values2[1]>1)
print corr_result_filter.first()
print corr_result_filter.countApprox(7200,0.8)
#corr_result_filter = corr_result.filter(lambda values2:values2[1]>1)
'''
