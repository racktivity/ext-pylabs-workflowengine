#!/opt/qbase3/bin/python
import os,sys
import pymonkey._init
from twisted.internet import protocol, reactor

print "Init q"
q,i=pymonkey._init.initialize(False)

print "Init w"
from workflowengine.CloudAPIActionManager import WFLActionManager
w=WFLActionManager()

print "Call"
print w.startRootobjectAction('application', 'addAccount', {})
print w.startRootobjectAction('application', 'addAccount', {})
print w.startRootobjectAction('application', 'addAccount', {})
print w.startRootobjectAction('application', 'addAccount', {})
