#!/usr/bin/env python

from constants import transports
from threading import Thread
import timeit
import xmlrpclib
import pprint

welcome = '''This is an extremely simple test-script, and it was originally meant to run 
some preliminary performance tests.
The only way to specificy what it does is to adapt the code.
The aim of the script was to compare performance between two versions of the application-server.
During development of this script, this was handled the quick-and-dirty way with two
separate installations of the Q-sandbox.

This specific script needs an applicationserver running on 127.0.0.1:8888 with the agent_service
installed. It does not need the sandbox to run,; it can run on a vanilla Python installation. 
It uses the first Python it finds in your PATH variable (can be either 2.5 or 2.6)

Please be aware that script does not start the application server !!!!
It does not operate in the sandbox environment at all.

'''

print welcome
           

class acallable:
    def __init__(self,p,i):
        self.proxy = xmlrpclib.ServerProxy(p)
        print 'creating callable for proxyaddress: %s' %p
        self.id = i
        self.count = 0
    def __call__(self):
        try:
            #rc = self.proxy.cloud_api_machine.list()
            rc = self.proxy.cloud_api_application.addAccount("","","","")
            pprint.pprint(rc)
            print 'Id %d:      Request %d' %(self.id, self.count)
            self.count += 1
        except Exception as e:
            print 'Ooops, something happended'
            raise e

datastore = dict()
class ExecThread(Thread):
    def __init__(self,id,datastore,repeat,number,callable):
        Thread.__init__(self)
        self.id = id
        self.repeat = repeat
        self.number = number
        self.callable =callable
        self.datastore = datastore
    def run(self):
        print '  Starting client thread %d' %(self.id,)
        timer = timeit.Timer(self.callable)
        results = timer.repeat(self.repeat,self.number)
        self.datastore[self.id] = results
        print '  Finished  client thread %d' %(self.id,)


sproxy = raw_input('Enter addess of the Server [http://admin:admin@127.0.0.1:8888] : ')
if not sproxy.strip():
    sproxy = 'http://admin:admin@127.0.0.1:8888'
nthreads = raw_input('Please enter the number of client threads [2] : ')
if not nthreads.strip():
    nthreads = 2
else:
    nthreads = int(nthreads.strip())

nrepeat  = raw_input('Please enter the number of times to repeat the test [2] : ')
if not nrepeat.strip():
    nrepeat = 2
else:
    nrepeat = int(nrepeat.strip())

nreq = raw_input('Please enter the number of requests for each test-series [10] : ')
if not nreq.strip():
    nreq = 10
else:
    nreq = int(nreq.strip())

raw_input('Please press enter to proceed, or Control-C to cancel. This is your last chance to cancel ...')
print

threadpool = []
for i in range(nthreads):
    t = ExecThread(i,datastore,nrepeat,nreq,acallable(sproxy,i))
    threadpool.append(t)

for i in threadpool:
    i.start()

for t in threadpool:
    t.join()

print
for i in datastore:
    print 'Results of thread %d' %(i,)
    print '  '
    for f in range(len(datastore[i])):
        print '  Subtest %d :  %.10f sec ' %(f,datastore[i][f])
    print '  Sum of times: %.10f' %(sum(datastore[i]),)
    print '  Average of times: %.10f' %(sum(datastore[i])/ len(datastore[i]),)
