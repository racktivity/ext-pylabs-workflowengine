from pymonkey import q

from workflowengine.Exceptions import ActionNotFoundException, WFLException, TimeOutException,ScriptFailedException
from workflowengine.WFLActionManager import ActorActionTaskletPath

from concurrence import Tasklet, Message

class MSG_ACTION_CALL(Message): pass
class MSG_ACTION_NOWAIT(Message): pass
class MSG_ACTION_RETURN(Message): pass
class MSG_ACTION_EXCEPTION(Message): pass

ActorActionScriptFolder = 'scripts'

class WFLAgentController:

    def __init__(self):
        self.__agentController = None

    def setAgentController(self, agentController):
        self.__agentController = agentController

    def executeScript(self, agentguid, actionname, scriptpath, params, executionparams={}, jobguid=None):
        """
        Execute a script on an agent. The action will be executed in a new job that is the newest child of the current job.
        The new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration and customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).

        @param agentguid:                      Guid of the agent on which the script will be executed.
        @type agentguid:                       guid

        @param actionname:                     The name of the action, will filled in, in the job
        @param actionname:                     string

        @param scriptpath:                     The path of the script, on the server.
        @type scriptpath:                      string

        @param params:                         Dictionary containing all parameters required to execute this script.
        @type params:                          dictionary

        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority
        @type executionParams:                 dictionary

        @param jobguid:                        Optional parameter, can be used to override the current job.
        @type jobguid:                         guid

        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary

        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised.
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:          If an exception occurres on the agent while executing the script.
        """
        #SETUP THE JOB AND THE PARAMS
        currentjobguid = jobguid or Tasklet.current().jobguid
        if q.workflowengine.jobmanager.isKilled(currentjobguid):
            raise Exception("Can't create child jobs: the job is killed !")

        params['jobguid'] = jobguid = q.workflowengine.jobmanager.createJob(currentjobguid, actionname, executionparams, agentguid, params=str(params))
        #START A NEW TASKLET FOR THE JOB
        q.workflowengine.jobmanager.startJob(jobguid)
        tasklet = Tasklet.new(self.__execute)(Tasklet.current(), jobguid, agentguid, scriptpath, params)
        #WAIT FOR THE ANSWER
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_ACTION_NOWAIT):
            return { 'jobguid':jobguid, 'result':None }
        if msg.match(MSG_ACTION_RETURN):
            return args[0]
        elif msg.match(MSG_ACTION_EXCEPTION):
            raise args[0]

    def executeActorActionScript(self, agentguid, scriptname, params, executionparams={}, jobguid=None):
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

        @param jobguid:                        Optional parameter, can be used to override the current job.
        @type jobguid:                         guid

        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary

        @raise ActionNotFoundException         If the script was not found.
        @raise IOError:                        If the scriptpath can't be read.
        @raise TimeOutException:               If the agent did not respond within the maxduration: the script will be killed and the exception will be raised.
        @raise AgentNotAvailableException:     If the agent is not available when starting the script, the script is not be started.
        @raise ScriptFailedException:             If an exception occurres on the agent while executing the script.
        """
        (actorname, actionname) = Tasklet.current().tags
        scriptpath = q.system.fs.joinPaths(ActorActionTaskletPath, actorname, actionname, ActorActionScriptFolder, scriptname + ".rscript")
        if not q.system.fs.exists(scriptpath):
            raise ActionNotFoundException("ActorActionScript", actorname+"."+actionname, scriptname)
        else:
            return self.executeScript(agentguid, actorname+"."+actionname+"."+scriptname, scriptpath, params, executionparams, jobguid)

    def killScript(self, agentguid, jobguid, timeout):
        self.__agentController.killScript(agentguid, jobguid, timeout)

    def __execute(self, parentTasklet, jobguid, agentguid, scriptpath, params):
        #SETUP THE CONTEXT
        Tasklet.current().jobguid = jobguid
        wait = q.workflowengine.jobmanager.shouldWait(jobguid)
        if wait is False: MSG_ACTION_NOWAIT.send(parentTasklet)()

        try:
            params = self.__agentController.executeScript(agentguid, jobguid, scriptpath, params)
        except TimeOutException, te:
            try:
                self.__agentController.killScript(agentguid, jobguid, 10)
            except TimeOutException:
                q.logger.log("Failed to kill Script '" + scriptpath + "' on agent '" + agentguid + "' for job '" + jobguid, 1)
            except ScriptFailedException:
                q.logger.log("Failed to execute Script '" + scriptpath + "' on agent '" + agentguid + "' for job '" + jobguid, 1)
            q.workflowengine.jobmanager.setJobDied(jobguid, te)
            if wait is True: MSG_ACTION_EXCEPTION.send(parentTasklet)(WFLException.create(te,jobguid))
        except Exception, e:
            q.workflowengine.jobmanager.setJobDied(jobguid, e)
            if wait is True: MSG_ACTION_EXCEPTION.send(parentTasklet)(WFLException.create(e,jobguid))
        else:
            q.workflowengine.jobmanager.setJobDone(jobguid, params.get('result'))
            if wait is True: MSG_ACTION_RETURN.send(parentTasklet)({'jobguid':jobguid, 'result':params.get('result')})

