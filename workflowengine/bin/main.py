import sys
sys.path = ['', '/opt/qbase3/lib/python25.zip', '/opt/qbase3/lib/python2.5', '/opt/qbase3/lib/python2.5/plat-linux2', '/opt/qbase3/lib/python2.5/lib-tk', '/opt/qbase3/lib/python2.5/lib-dynload', '/opt/qbase3/lib/python2.5/site-packages', '/opt/qbase3/lib/python/site-packages', '/opt/qbase3/lib/pymonkey/core', '/opt/qbase3/lib/pymonkey/core/PyMonkey-4.0.1-py2.5.egg', '/opt/qbase3/lib/python/site-packages/osis-0.1-py2.5.egg', '/opt/qbase3/lib/python/site-packages/configobj-4.5.3-py2.5.egg', '/opt/qbase3/lib/python2.5/site-packages/concurrence-0.3.1-py2.5-linux-i686.egg']

from signal import signal, SIGTERM
from sys import exit

from pymonkey.InitBaseCore import q, i
from pymonkey.tasklets import TaskletsEngine
from pymonkey.log.LogTargets import LogTargetFileSystem 

from concurrence import Tasklet, Message, dispatch

from workflowengine.DRPClient import DRPTask
from workflowengine.SocketServer import SocketTask
from workflowengine.AgentController import AgentControllerTask 
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.WFLJob import WFLJob
from workflowengine.Exceptions import WFLException

import workflowengine.ConcurrenceSocket as ConcurrenceSocket
ConcurrenceSocket.install()

def main():
    
    try:
        #INITIALIZE THE APPLICATION
        q.application.appname = "workflowengine"
        q.logger.addLogTarget(LogTargetFileSystem(maxverbositylevel=5))        
        q.logger.addLogTarget(WFLJobLogTarget())
        config = i.config.workflowengine.getConfig('main')
        q.workflowengine.actionmanager.init()
        
        #INITIALIZE THE TASKS
        socket_task = SocketTask(int(config['port']))
        def _handle_message(data):
            try:
                ret = q.workflowengine.actionmanager.startRootobjectAction(data['rootobjectname'], data['actionname'], data['params'], data['executionparams'], data['jobguid'])
                socket_task.sendData({'id':data['id'], 'error':False, 'return':ret})
            except Exception, e:
                socket_task.sendData({'id':data['id'], 'error':True, 'exception':WFLException.create(e)})
        socket_task.setMessageHandler(_handle_message)
        
        drp_task = DRPTask(config['osis_address'], config['osis_service'])
        ac_task = AgentControllerTask(config['agentcontrollerguid'], config['xmppserver'], config['password'])
    except Exception, e:
        q.logger.log("[SL_WFL] Initialization failed: " + str(e), 1)
        print str(e)
        exit(-1)
    else:
        #SETUP THE SIGNAL HANDLER: CLOSE THE SOCKET ON EXIT
        def sigterm_received():
            q.logger.log('Received SIGTERM: shutting down.')
            socket_task.stop()
            exit(-SIGTERM)
        signal(SIGTERM, lambda signum, stack_frame: sigterm_received())
        
        #START THE TASKS AND REGISTER THEM IN THE Q-TREE
        socket_task.start()
        
        drp_task.start()
        drp_task.connectDRPClient(q.drp)
        
        ac_task.start()
        ac_task.connectWFLAgentController(q.workflowengine.agentcontroller)
        
        print "Ready !"
    
if __name__=='__main__':
    dispatch(main)
