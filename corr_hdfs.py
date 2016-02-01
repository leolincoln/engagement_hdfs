from pyspark import SparkContext
import os
#import numpy as np
import math
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
    timeseries = values[3]
    return ((x,y,z),timeseries)
def  print_rdd(rdd):
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
sc = SparkContext()
hdfsPrefix = 'hdfs://wolf.iems.northwestern.edu/user/huser54/'
fileName1 = 'engagement/HitchcockData0.dat'
fileName2 = 'engagementsample/'
lines = sc.textFile(hdfsPrefix+fileName1)
values = lines.map(value_pairs)
print 'values obtained'
print values.first()
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
#outname = hdfsPrefix+'engagementprune/'

#os.system('hdfs dfs -rm -r '+outname)
#print count_rdd(corr_result_filter)
#corr_result_filter.saveAsTextFile(outname)

