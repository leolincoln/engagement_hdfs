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
    if len(tableValues)<5:
        print 'not valid table values, cannot preapre query. '
        return ''
    result = '   '
    result += 'INSERT INTO ' + str(keyspace)+'.'+str(tableName) + tableColumns+ 'values('
    for value in tableValues:
        result +=str(value) + ','
    result = result[:-1];
    result += ');'
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

def prepareCreateTable(tableName,keyspace='engagement',valueNames=['subject','x','y','z','time','data'],valueTypes=['int','int','int','int','int','int'],primaryKeys=['subject','time','x','y','z'],comment='engagement'):
    '''
        CREATE TABLE engagement.engagement_s1(
        time int,
        x int,
        y int,
        z int,
        subject int,
        data int,
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
    
def getSession(address = ['10.10.10.10'],username='cassandra', password='cassandra'):
    auth_provider = PlainTextAuthProvider(username, password)
    cluster = Cluster(['10.10.10.10'],auth_provider=auth_provider, protocol_version=2)
    session = cluster.connect('engagement')
    return session
