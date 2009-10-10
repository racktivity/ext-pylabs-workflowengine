import sys, traceback
sys.path.append('/opt/qbase3/lib/python2.5/site-packages/concurrence-0.3.1-py2.5-linux-i686.egg')

from pymonkey.InitBaseCore import q, i
from pymonkey.tasklets import TaskletsEngine
from pymonkey.log.LogTargets import LogTargetFileSystem

q.application.appname = "workflowengine"

from concurrence import Tasklet, Message, dispatch

try:
    import json
except ImportError:
    import simplejson as json

from workflowengine.DRPClient import DRPTask
from workflowengine.AgentController import AgentControllerTask
from workflowengine.AppServer import AppServerTask

from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.Exceptions import WFLException

import workflowengine.ConcurrenceSocket as ConcurrenceSocket
ConcurrenceSocket.install()

initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initSuccess')
initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initFailed')

#LOAD THE TASKLETS OUTSIDE THE DISPATCH: 10 TIMES FASTER.
q.workflowengine.actionmanager.init()

def main():

    try:
        #INITIALIZE THE APPLICATION
        q.logger.addLogTarget(LogTargetFileSystem(maxverbositylevel=5))
        q.logger.addLogTarget(WFLJobLogTarget())
        config = i.config.workflowengine.getConfig('main')

        #INITIALIZE THE TASKS
        drp_task = DRPTask(config['osis_address'], config['osis_service'])
        ac_task = AgentControllerTask(config['agentcontrollerguid'], config['xmppserver'], config['password'])
        
        appserver_task = AppServerTask(int(config['port']))
        appserver_task.addSerializer('json', json.dumps, json.loads)
        def cloud_api_handler(call):
            return q.workflowengine.actionmanager.startRootobjectAction(str(call['rootobject']), str(call['action']), call['params'], call['executionparams'], call['jobguid'])
        appserver_task.addCallHandler('cloud_api', cloud_api_handler)
        
    except Exception, e:
        q.logger.log("[SL_WFL] Initialization failed: " + str(e), 1)
        traceback.print_exc()
        q.system.fs.createEmptyFile(initFailedFile)
        sys.exit(-1)
    else:
        q.system.fs.createEmptyFile(initSuccessFile)

        #START THE TASKS AND REGISTER THEM IN THE Q-TREE        
        drp_task.start()
        drp_task.connectDRPClient(q.drp)

        ac_task.start()
        ac_task.connectWFLAgentController(q.workflowengine.agentcontroller)
        
        appserver_task.start()

        print "Ready !"

if __name__ == '__main__':
    dispatch(main)
