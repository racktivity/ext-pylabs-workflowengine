from pymonkey import q

from workflowengine.Exceptions import ScriptFailedException, AgentNotAvailableException, TimeOutException 
from workflowengine.WFLJob import WFLJob
from workflowengine.WFLActionManager import ActorActionTaskletPath

from concurrence import Tasklet

ActorActionScriptFolder = '/scripts/'

class WFLAgentController:
    
    def __init__(self):
        self.__agentController = None
        
    def setAgentController(self, agentController):
        self.__agentController = agentController
        
    def executeScript(self, agentguid, scriptpath, params, executionparams={}):
        """        
        Execute a script on an agent. The action will be executed in a new job that is the newest child of the current job.
        The new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration and customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        
        @param agentguid:                      Guid of the agent on which the script will be executed.
        @type agentguid:                       guid
        
        @param scriptpath:                     The path of the script, on the server.
        @type scriptpath:                      string 
        
        @param params:                         Dictionary containing all parameters required to execute this script. 
        @type params:                          dictionary
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority 
        @type executionParams:                 dictionary
        
        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised. 
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:          If an exception occurres on the agent while executing the script.
        """
        job = WFLJob(parentjob=Tasklet.current().job.job, actionName=scriptpath, executionparams=executionparams, agentguid=agentguid)
        timeout = job.getMaxduration()
        
        try:
            output = self.__agentController.executeScript(agentguid, job.getJobGUID(), scriptpath, params, timeout, job.log)
            if output['error'] == False:
                params = output['params']
            else:
                job.raiseError("ERROR OCCURRED: errorcode=" + str(output['errorcode']) +", " + output['erroroutput'])
                raise ScriptFailedException(job.getJobGUID(), agentguid, scriptpath, output['errorcode'], output['erroroutput'])
        except IOError, ioe:
            job.raiseError(ioe)
            raise
        except AgentNotAvailableException, anae:
            job.raiseError(anae)
            raise
        except TimeOutException, te:
            try:
                self.__agentController.killScript(agentguid, job.getJobGUID(), 10)
            except TimeOutException:
                q.logger.log("Failed to kill Script '" + scriptpath + "' on agent '" + agentguid + "' for job '" + job.getJobGUID(), 1)
            
            (te.jobguid, te.agentguid, te.scriptpath, te.timeout) = (job.getJobGUID(), agentguid, scriptpath, timeout)
            job.raiseError(te)
            raise
        else:
            job.done()
            return {'result':params['result'], 'jobguid':job.getJobGUID()}

    def executeActorActionScript(self, agentguid, scriptname, params, executionparams={}):
        """
        Execute an actor action script on an agent. The action will be executed in a new job that is the newest child of the current job.
        The new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration and customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        The scripts are found in $actoractionpath/$actorname/$actionname/scripts/${scriptname}.rscript
        
        
        @param agentguid:                      Guid of the agent on which the script will be executed.
        @type agentguid:                       guid
        
        @param scriptname:                     The name of the script. Extension .rscript will be added automatically.
        @type scriptpath:                      string 
        
        @param params:                         Dictionary containing all parameters required to execute this script. 
        @type params:                          dictionary
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority 
        @type executionParams:                 dictionary
        
        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised. 
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:             If an exception occurres on the agent while executing the script.
        """
        (actorname, actionname) = Tasklet.current().tags
        scriptpath = ActorActionTaskletPath + actorname + "/" + actionname + ActorActionScriptFolder + scriptname + ".rscript"
        return self.executeScript(agentguid, scriptpath, params, executionparams)
