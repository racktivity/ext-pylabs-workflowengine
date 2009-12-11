#!/opt/qbase3/bin/python
import os,sys
import pymonkey._init
from twisted.internet import protocol, reactor

print "Init q"
q,i=pymonkey._init.initialize(False)
print "Init q done"

from workflowengine.CloudAPIActionManager import WFLActionManager

print "Init w"
w=WFLActionManager()
print "Init w done"

def do():
    print "Call w"
    #application.addAccount("","","","")
    print w.startRootobjectAction('application', 'addAccount', {})
    print "Call w done"

reactor.callInThread(do)
reactor.run()

