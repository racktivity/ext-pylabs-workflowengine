#!/opt/qbase3/bin/python
#vim:fdl=0:
import time
if __name__=='__main__':
    from pymonkey.InitBase import q,i
else:
    from pymonkey import q,i

if __name__=='__main__':
    c=i.config.cloudApiConnection.find("main")
    t0=time.time()
    print t0
    for i in range(10):
        print c.clouduser.find()
    t1=time.time()
    print t1
    print "time: ",t1-t0
