# -*- coding: utf-8 -*-
"""
Created on Tue Oct 28 20:49:24 2014

@author: leoliu
"""
import numpy as np
import os
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider


def prepareInsert(keyspace,tableName, tableValues,tableColumns =' (x,y,z,time,data) ' ):
    result = '   '
    result += 'INSERT INTO ' + str(keyspace)+'.'+str(tableName) + tableColumns+ 'values('
    for value in tableValues:
        import numpy as np
        if type(value) is type(np.array([])):
            valueStr = arrToStr(value)
        else:
            valueStr = str(value)
        result +=valueStr+ ','
    result = result[:-1];
    result += ');'
    return result

def arrToStr(inArr):
    result = '['
    for value in inArr:
        result+=str(value)
        result+=','
    result = result[:-1]
    result+=']'
    return result


'''
CREATE TABLE engagement.engagement_s1(
    x int,
    y int,
    z int,
    time int,
    data int,
    PRIMARY KEY (x,y,z,time)
)
WITH comment='engagement_s1'
AND read_repair_chance = 1.0;
'''     

def prepareCreateTable(tableName,keyspace='engagement',valueNames=['subject','x','y','z','data'],valueTypes=['int','int','int','int','list<int>'],primaryKeys=['x','subject','y','z'],comment='engagement'):
    '''
        CREATE TABLE engagement.engagement_s1(
        x int,
        y int,
        z int,
        subject int,
        data list,
        PRIMARY KEY (subject,time,x,y,z)
    )
    WITH comment='engagement_s1'
    AND read_repair_chance = 1.0;
    '''
    result = '   '
    if len(valueNames) != len(valueTypes):
        print 'cannot create table -- value names and value types should have the same length'
        return ''

    result+= 'CREATE TABLE IF NOT EXISTS '+keyspace + '.'+ tableName +'('
    for index in range(len(valueNames)):
        result+=valueNames[index] + ' ' + valueTypes[index] + ','
    result += 'PRIMARY KEY ('
    for key in primaryKeys:
        result +=key + ','
    result = result[:-1]
    result +=')) WITH comment=\''+comment+'\' AND read_repair_chance = 1.0;'
    return result
    
def getSession(address = ['cub0','cub1','cub2','cub3'],username='leolincoln', password='ll7713689'):
    auth_provider = PlainTextAuthProvider(username, password)
    cluster = Cluster(address,auth_provider=auth_provider)
    session = cluster.connect('engagement')
    return session
