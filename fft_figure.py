#this file is for reading 5 signals from a csv file, 
#formatted as x;y;z;t[0],t[1].....t[600]

from neuro_athelets import analytics_engine as a
import numpy as np
from matplotlib import pylab as plt
data =[]
names = []
i=0
with open('20_2.csv','r') as f:
    for line in f:
        data.append(line.split(';')[-1].split(','))
        names.append('_'.join(line.split(';')[:-1]))
        i+=1
#plot the 0th array
for i in range(len(data)):
    f,ax= plt.subplots()
    x,y = a.fft(data[i])
    ax.plot(x,y)
    f.show(False)
    f.savefig(names[i])
