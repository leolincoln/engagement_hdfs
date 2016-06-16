import pandas as pd
file_name = 'dimxyzpair.csv'
data = pd.read_csv('HitchcockData0raw.csv',header=None,names=['xyzKey','timeKey','subjectKey','data'])
data = data['xyzKey'].unique()
with open(file_name,'a') as f:
    for i in xrange(len(data)):
        for j in xrange(i,len(data)):
            f.write(data[i]+','+data[j]+'\n')
        print 'on line'+str(i)+'/'+str(len(data))
    
