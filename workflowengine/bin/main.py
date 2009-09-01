from pymonkey.InitBaseCore import q
from concurrence import Tasklet, Message, dispatch

from workflowengine.DRPClient import DRPTask
from workflowengine.SocketServer import SocketTask
from workflowengine.AgentController import AgentControllerTask 
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.WFLJob import WFLJob

import workflowengine.ConcurrenceSocket as ConcurrenceSocket
ConcurrenceSocket.install()

def main():
    try:
        q.logger.addLogTarget(WFLJobLogTarget())
        
        socket_task = SocketTask(9876)
        def _handle_message(data):
            try:
                ret = q.workflowengine.actionmanager.startRootobjectAction(data['rootobjectname'], data['actionname'], data['params'], data['executionparams'], data['jobguid'])
                socket_task.sendData({'id':data['id'], 'error':False, 'return':ret})
            except Exception, e:
                socket_task.sendData({'id':data['id'], 'error':True, 'exception':e})
        socket_task.setMessageHandler(_handle_message)
        
        drp_task = DRPTask('http://127.0.0.1:8888', 'osis_service')
        ac_task = AgentControllerTask('agentcontroller', 'host147.office.aserver.com', 'test')
        
    except Exception, e:
        q.logger.log("[SL_WFL] Initialization failed: " + str(e), 1)
        print str(e)
        quit()
    else:
        socket_task.start()
        
        drp_task.start()
        drp_task.connectDRPClient(q.drp)
        
        ac_task.start()
        ac_task.connectWFLAgentController(q.workflowengine.agentcontroller)
        
        print "Ready !"
    
if __name__=='__main__':
    dispatch(main)
