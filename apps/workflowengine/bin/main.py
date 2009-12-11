import sys, traceback
from signal import signal, SIGTERM

from pymonkey.InitBaseCore import q, i

from pymonkey.tasklets import TaskletsEngine
#from pymonkey.logging.logtargets.LogTargetScribe import LogTargetScribe

q.application.appname = "workflowengine"

from concurrence import Tasklet, Message, dispatch

from workflowengine.DRPClient import DRPTask
from workflowengine.SocketServer import SocketTask
from workflowengine.AgentController import AgentControllerTask
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.Exceptions import WFLException

import workflowengine.ConcurrenceSocket as ConcurrenceSocket
ConcurrenceSocket.install()

initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initSuccess')
initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initFailed')

#LOAD THE TASKLETS OUTSIDE THE DISPATCH: 10 TIMES FASTER.
q.workflowengine.actionmanager.init()

def alog(*l):
    f=file('/tmp/l','a')
    f.write('%r\n'%l)
    f.close()

def main():

    try:
        #INITIALIZE THE APPLICATION
        q.logger.logTargetAdd(WFLJobLogTarget())
        #q.logger.logTargetAdd(LogTargetScribe())

        config = i.config.workflowengine.getConfig('main')

        #INITIALIZE THE TASKS
        socket_task = SocketTask(int(config['port']))
        def _handle_message(data):
            try:
                q.logger.log('Received message from CloudAPI with id %s - %s.%s' % (data['id'], data['rootobjectname'], data['actionname']))
                ret = q.workflowengine.actionmanager.startRootobjectAction(data['rootobjectname'], data['actionname'], data['params'], data['executionparams'], data['jobguid'])
                q.logger.log('Sending result message to CloudAPI for id %s - %s.%s' % (data['id'], data['rootobjectname'], data['actionname']))
                socket_task.sendData({'id':data['id'], 'error':False, 'return':ret})
            except Exception, e:
                socket_task.sendData({'id':data['id'], 'error':True, 'exception':WFLException.create(e)})
        socket_task.setMessageHandler(_handle_message)

        drp_task = DRPTask(config['osis_address'], config['osis_service'])
        hostname = config['hostname'] if 'hostname' in config and config['hostname'] else config['xmppserver']
        ac_task = AgentControllerTask(config['agentcontrollerguid'], config['xmppserver'], hostname, config['password'])
    except Exception, e:
        q.logger.log("[SL_WFL] Initialization failed: " + str(e), 1)
        traceback.print_exc()
        q.system.fs.createEmptyFile(initFailedFile)
        sys.exit(-1)
    else:
        q.system.fs.createEmptyFile(initSuccessFile)

        #SETUP THE SIGNAL HANDLER: CLOSE THE SOCKET ON EXIT
        def sigterm_received():
            q.logger.log('Received SIGTERM: shutting down.')
            socket_task.stop()
            sys.exit(-SIGTERM)
        signal(SIGTERM, lambda signum, stack_frame: sigterm_received())

        #START THE TASKS AND REGISTER THEM IN THE Q-TREE
        socket_task.start()

        drp_task.start()

        alog("drp_task connectDRPClient PRE")
        drp_task.connectDRPClient(q.drp)
        alog("drp_task connectDRPClient POST")

        ac_task.start()
        ac_task.connectWFLAgentController(q.workflowengine.agentcontroller)

        print "Ready !"

if __name__=='__main__':
    dispatch(main)
