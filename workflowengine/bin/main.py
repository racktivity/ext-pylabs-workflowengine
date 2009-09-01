from pymonkey.InitBaseCore import q, i
from concurrence import Tasklet, Message, dispatch

from workflowengine.DRPClient import DRPTask
from workflowengine.SocketServer import SocketTask
from workflowengine.AgentController import AgentControllerTask 
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.WFLJob import WFLJob

import workflowengine.ConcurrenceSocket as ConcurrenceSocket
ConcurrenceSocket.install()

def testDrpTasklet(num):
    job1 = q.drp.job.new()
    job1.name = "testen"
    q.drp.job.save(job1)
    print str(num) + ": JobGUID1 = " + job1.guid
    
    job2 = q.drp.job.new()
    job2.name = "testen"
    job2.parentjobguid = job1.guid
    job2.order = 5
    q.drp.job.save(job2)
    print str(num) + ": JobGUID2 = " + job2.guid
    
    WFLJob.printJobTree(job2.guid)
    print "Next Child: " + str(WFLJob.getNextChildOrder(job2.guid))
    
def nonBlockingChecker():
    while True:
        print "."
        Tasklet.sleep(0.1)

def testing():
    Tasklet.sleep(3)
    ret = q.workflowengine.actionmanager.startRootobjectAction("ro_test", "test", {'num':10})
    print ret['result']
    WFLJob.printJobTree(ret['jobguid'])
    
    #job = q.drp.job.new()
    #q.drp.job.save(job)
    #Tasklet.sleep(3)
    #ret = q.workflowengine.agentcontroller.executeActorActionScript('agent1', 'aa_test', 'test', 'test_agent', {'input':'hello'}, job.guid)
    #print str(ret)


def main():
    try:
        q.logger.addLogTarget(WFLJobLogTarget())
        config = i.config.workflowengine.getConfig('main')
        
        socket_task = SocketTask(int(config['port']))
        def _handle_message(data):
            try:
                ret = q.workflowengine.actionmanager.startRootobjectAction(data['rootobjectname'], data['actionname'], data['params'], data['executionparams'], data['jobguid'])
                socket_task.sendData({'id':data['id'], 'error':False, 'return':ret})
            except Exception, e:
                socket_task.sendData({'id':data['id'], 'error':True, 'exception':e})
        socket_task.setMessageHandler(_handle_message)
        
        drp_task = DRPTask(config['osis_address'], config['osis_service'])
        ac_task = AgentControllerTask(config['agentcontrollerguid'], config['xmppserver'], config['password'])
        
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
        #test_task = Tasklet.new(testing)()
        
        #test_task = Tasklet.new(testDrpTasklet)(1)
        #checker_task = Tasklet.new(nonBlockingChecker)()
    
if __name__=='__main__':
    dispatch(main)
