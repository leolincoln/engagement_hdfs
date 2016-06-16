'''
This script monitors screen xyz. If the file size at file_name exceeds certain size, then this program will force the detached screen to quit. 
'''
#Usage: python kill_xyz_screen.py
import os,time
#the file_name to be monitored on
file_name = 'dimxyzpair.csv'
while(True):
    #in size in GB
    size = os.path.getsize(file_name)/1000.0/1000.0/1000.0
    #the screen size to be monitored on. In GB. 
    if size>=50:
        os.system('screen -X -S xyz quit')
        print 'screen xyz killed at size: ',size,'GB'
        break
    else:
        print 'current size:',size
    time.sleep(10)
