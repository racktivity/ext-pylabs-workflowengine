import sys
import os
import traceback

from signal import signal, SIGTERM

from pylabs.InitBaseCore import q, p

#from pylabs.tasklets import TaskletsEngine
#from pylabs.logging.logtargets.LogTargetScribe import LogTargetScribe

q.application.appname = "workflowengine"

from concurrence import Tasklet, dispatch

from workflowengine.DRPClient import DRPTask
from workflowengine.SocketServer import SocketTask
from workflowengine.AgentController import AgentControllerTask
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.Exceptions import WFLException
from workflowengine.DebugInterface import DebugInterface

from workflowengine import getAppName, getPort

#import workflowengine.ConcurrenceSocket as ConcurrenceSocket
#ConcurrenceSocket.install()

initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
    'workflowengine.%s.initSuccess' % getAppName())
initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
    'workflowengine.%s.initFailed' % getAppName())

#LOAD THE TASKLETS OUTSIDE THE DISPATCH: 10 TIMES FASTER.
q.workflowengine.actionmanager.init()
q.workflowengine.jobmanager.init()

def getConfig():
    config = {}

    # Convention-over-configuration
    # AKA: hard-coded configuration, in this case
    # These 2 aren't used anyway
    config['osis_address'] = 'http://localhost/appserver/%s/xmlrpc' % \
        getAppName()
    config['osis_service'] = 'osis'
    config['hostname'] = getAppName()
    config['agentcontrollerguid'] = 'agentcontroller'
    config['xmppserver'] = 'localhost'
    config['password'] = 'agentcontroller'

    config['enable_debug'] = '--debug' in sys.argv

    config['port'] = getPort()

    return config

def main():

    p.api = p.application.getAPI(getAppName(), context=q.enumerators.AppContext.WFE)

    try:
        #config = i.config.workflowengine.getConfig('main')
        config = getConfig()

        enable_debug = config['enable_debug'] == 'True' if 'enable_debug' in config else False

        #INITIALIZE THE APPLICATION
        q.logger.logTargetAdd(WFLJobLogTarget())
        #q.logger.logTargetAdd(LogTargetScribe())

        #INITIALIZE THE TASKS
        socket_task = SocketTask(int(config['port']))
        def _handle_message(data, connection):
            try:
                q.logger.log('Received message from CloudAPI with id %s - %s.%s.%s' % (data['id'], data['domainname'], data['rootobjectname'], data['actionname']), level=8)
                ret = q.workflowengine.actionmanager.startRootobjectAction(data['domainname'], data['rootobjectname'], data['actionname'], data['params'], data['executionparams'], data['jobguid'])
                q.logger.log('Sending result message to CloudAPI for id %s - %s.%s.%s' % (data['id'], data['domainname'], data['rootobjectname'], data['actionname']), level=8)
                connection.sendData({'id':data['id'], 'error':False, 'return':ret})
            except Exception, e:
                connection.sendData({'id':data['id'], 'error':True, 'exception':WFLException.create(e)})
        socket_task.setMessageHandler(_handle_message)

        if enable_debug:
            debug_socket_task = SocketTask(1234) #TODO Read the port from a config file
            debugInterface = DebugInterface(debug_socket_task)
            debug_socket_task.setMessageHandler(debugInterface.handleMessage)
            q.workflowengine.jobmanager.initializeDebugging()

        drp_task = DRPTask(config['osis_address'], config['osis_service'])
        drp_job_task = DRPTask(config['osis_address'], config['osis_service'])
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

            try:
                socket_task.stop()
            except Exception, exc:
                q.logger.log(
                    'Error while shutting down socket task: %s' % exc, 1)
            except:
                q.logger.log('Error while shutting down socket task', 1)

            try:
                sys.exit(-SIGTERM)
            except Exception, exc:
                q.logger.log('Error while calling sys.exit: %s' % exc, 1)
            except:
                q.logger.log('Error while calling sys.exit', 1)

            # This should not be reached, unless something goes wrong above
            os._exit(-SIGTERM)

        signal(SIGTERM, lambda signum, stack_frame: sigterm_received())

        #START THE TASKS AND REGISTER THEM IN THE Q-TREE
        socket_task.start()
        if enable_debug:debug_socket_task.start()

        drp_task.start()
        drp_task.connectDRPClient(p.api.model)

        drp_job_task.start()
        drp_job_task.connectDRPClient(p.api.model, (('core', 'job'),))

        ac_task.start()
        ac_task.connectWFLAgentController(q.workflowengine.agentcontroller)
        
        def clean_jobs():
            # Clean out job db if required
            # Put all running jobs in error with log msg
            from pylabs.messages.LogObject import LogObject
            from pylabs.messages import toolStripNonAsciFromText
            
            f = p.api.model.core.job.getFilterObject()
            f.add('core_view_job_list', 'jobstatus', 'RUNNING')
            running_jobs = p.api.model.core.job.find(f)
            
            q.logger.log('%s jobs to reset' % len(running_jobs), 1)
            
            if len(running_jobs) > 0:
                
                msg = toolStripNonAsciFromText('\n\nJob status reset to ERROR during workflowengine initialization')
                l = LogObject()
                l.init(msg, 1)
                logentry = q.logger._encodeLog(l.getMessageString(), level=1)
                
                for jobguid in running_jobs:
                    try:
                        job = p.api.model.core.job.get(jobguid)
                        job.jobstatus = p.api.model.enumerators.jobstatus.ERROR
                        job.log = ( job.log or "") + logentry
                        q.logger.log('Setting running job %s to ERROR' % job.guid, 1)
                        p.api.model.core.job.save(job)
                    except Exception, e:
                        q.logger.log('Failed to reset job %s: %s' % (jobguid, ex.message), 1)
        
        tasklet = Tasklet.new(clean_jobs)()
        Tasklet.join(tasklet)
        
        print "Ready !"

if __name__=='__main__':
    dispatch(main)
