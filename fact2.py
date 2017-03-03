import pandas as pd
data = pd.read_csv('HitchcockData0raw.csv',header=None,names=['xyzKey','timeKey','subjectKey','data']).head(10000)
#data = data['xyzKey']
df = pd.merge(data,data,on=['subjectKey','timeKey'])
df_filter= pd.DataFrame([v for v in zip(df.xyzKey_x,df.xyzKey_y,df.timeKey,df.subjectKey,df.data_x,df.data_y) if v[0]!=v[1] and v[0]<v[1]])
df_filter.to_csv('fact2_subject0_part.csv',header=False,index=False)
