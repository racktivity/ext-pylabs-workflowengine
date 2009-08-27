from pymonkey import q, i
from workflowengine.agentcontroller.agentcontroller import AgentController
from workflowengine.agentcontroller.agentcontroller import AgentNotAvailableException
from workflowengine.agentcontroller.agentcontroller import TimeOutException
from workflowengine.Exceptions import ScriptFailedException
from workflowengine.WFLJob import WFLJob
from workflowengine.WFLLogTargets import WFLJobLogTarget
from workflowengine.WFLActionManager import ActorActionTaskletPath

def inAppserver():
    import threading
    return hasattr(q.application, '_store') and isinstance(q.application._store, threading.local)

ActorActionScriptFolder = '/scripts/'

class WFLAgentController:
    
    def __init__(self):
        if not inAppserver():
            raise Exception("The agentcontroller cannot be used outside the applicationserver ! Twisted is required...")
        
        config = i.config.agentcontroller.getConfig('main')
        agentcontrollerID = config['agentcontrollerid']
        xmppServer = config['xmppserver']
        password = config['password']
        self.__agentController = AgentController(agentcontrollerID, xmppServer, password)
    
    def executeScript(self, agentguid, scriptpath, params, jobguid, executionparams={}):
        """        
        Execute a script on an agent. The action will be executed in a new job that is the newest child of the job.
        The new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration and customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        
        @param agentguid:                      Guid of the agent on which the script will be executed.
        @type agentguid:                       guid
        
        @param scriptpath:                     The path of the script, on the server.
        @type scriptpath:                      string 
        
        @param params:                         Dictionary containing all parameters required to execute this script. 
        @type params:                          dictionary
        
        @param jobguid:                        Guid of the job
        @type obguid:                          guid
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority 
        @type executionParams:                 dictionary
        
        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised. 
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:          If an exception occurres on the agent while executing the script.
        """
        
        oldLogTarget = isinstance(q.logger.logTargets[-1], WFLJobLogTarget) and q.logger.logTargets.pop()
        
        job = WFLJob(parentjobguid=jobguid, actionName=scriptpath, executionparams=executionparams, agentguid=agentguid)
        timeout = job.getMaxduration()
        
        try:
            output = self.__agentController.executeScript(agentguid, job.getJobGUID(), scriptpath, params, timeout, job.log)
            if output['error'] == False:
                params = output['params']
            else:
                job.raiseError("ERROR OCCURRED: errorcode=" + str(output['errorcode']) +", " + output['erroroutput'])
                raise ScriptFailedException(output['errorcode'], output['erroroutput'])
        except IOError:
            job.raiseError("IOError on script '" + scriptpath + "'.")
            oldLogTarget and q.logger.addLogTarget(oldLogTarget) 
            raise
        except AgentNotAvailableException:
            job.raiseError("Agent not available.")
            oldLogTarget and q.logger.addLogTarget(oldLogTarget)
            raise
        except TimeOutException:
            self.__agentController.killScript(agentguid, job.getJobGUID(), 10)
            job.raiseError("Execution of the script timed out.")
            oldLogTarget and q.logger.addLogTarget(oldLogTarget) 
            raise
        else: 
            job.done()
            oldLogTarget and q.logger.addLogTarget(oldLogTarget)
                
            return {'result':params, 'jobguid':job.getJobGUID()}

    def executeActorActionScript(self, agentguid, actorname, actionname, scriptname, params, jobguid, executionparams={}):
        """        
        Execute an actor action script on an agent. The action will be executed in a new job that is the newest child of the job.
        The new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration and customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        The scripts are found in $actoractionpath/$actorname/$actionname/scripts/${scriptname}.rscript
        
        
        @param agentguid:                      Guid of the agent on which the script will be executed.
        @type agentguid:                       guid
        
        @param actorame:                       The name of the actor.
        @type actorname:                       string
        
        @param actionname:                     The name of the action.
        @type actionname:                      string 
        
        @param scriptname:                     The name of the script. Extension .rscript will be added automatically.
        @type scriptpath:                      string 
        
        @param params:                         Dictionary containing all parameters required to execute this script. 
        @type params:                          dictionary
        
        @param jobguid:                        Guid of the job
        @type jobguid:                         guid
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority 
        @type executionParams:                 dictionary
        
        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised. 
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:             If an exception occurres on the agent while executing the script.
        """
        scriptpath = ActorActionTaskletPath + actorname + "/" + actionname + ActorActionScriptFolder + scriptname + ".rscript"
        return self.executeScript(agentguid, scriptpath, params, jobguid, executionparams)

    
    def agents(self):
        return self.__agentController.listAvailableAgents()
