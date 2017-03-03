import pandas as pd
data = pd.read_csv('HitchcockData0raw.csv',header=None,names=['xyzKey','timeKey','subjectKey','data'])
data['data'] = data['data']**2
data.to_csv('fact3_subject0.csv',header=False,index=False)
