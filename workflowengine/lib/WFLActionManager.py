from pymonkey import q
from pymonkey.tasklets import TaskletsEngine
from workflowengine.WFLJob import WFLJob
from workflowengine.Exceptions import ActionNotFoundException
from workflowengine.WFLLogTargets import WFLJobLogTarget

ActionManagerTaskletPath = '/opt/qbase3/apps/workflowengine/tasklets/'
ActorActionTaskletPath = ActionManagerTaskletPath + 'actor/'
RootobjectActionTaskletPath = ActionManagerTaskletPath + 'rootobject/'

class WFLActionManager():
    """
    This class is hosted on q.workflowengine.actionmanager.
    Will result in a workflow being executed which implements the action: the worklflows are tasklet scripts.

    Location of root object actions:
    ##/opt/qbase3/apps/workflowengine/tasklets/rootobject/$actorname/$actionname/

    Location of actor actions:
    ##/opt/qbase3/apps/workflowengine/tasklets/actor/$actorname/$actionname/
    """

    def __init__(self):
    	self.__taskletEngine = TaskletsEngine()
	##check if directory exists before adding it
	if not q.system.fs.exists(ActorActionTaskletPath):
	    q.system.fs.createDir(ActorActionTaskletPath)

    	self.__taskletEngine.addFromPath(ActorActionTaskletPath)

	##check if directory exists before adding it
	if not q.system.fs.exists(RootobjectActionTaskletPath):
	    q.system.fs.createDir(RootobjectActionTaskletPath)

    	self.__taskletEngine.addFromPath(RootobjectActionTaskletPath)

    def startActorAction(self, actorname, actionname, params, jobguid, executionparams={}):
        """
        Starts a given Actor Action. Uses the tasklet engine to run the matching tasklets.
        The action will be executed in a new job that is the newest child of the job.
        If a job is provided, the new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration, customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        The guid of the job in which the action is executed, will be added to params: params['jobguid'].

        @param actorname:                      Name of the actor of the actor action.
        @type actorname:                       string

        @param actionnmame:                    Name of the action of the actor action.
        @type actionname:                      string

        @param params:                         Dictionary containing all parameters required to execute this actions.
        @type params:                          dictionary

        @param jobguid:                        Guid of the parent job.
        @type jobguid:                         guid

        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, customerguid, wait, timetostart, priority
        @type executionParams:                 dictionary

        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary

        @raise ActionNotFoundException:        In case no actions are found for the specified actorname and actoraction
        @raise e:                              In case an error occurred, exception is raised
        """
        if len(self.__taskletEngine.find(tags=(actorname, actionname), path=ActorActionTaskletPath)) == 0:
    		raise ActionNotFoundException("ActorAction", actorname, actionname)

        oldLogTarget = isinstance(q.logger.logTargets[-1], WFLJobLogTarget) and q.logger.logTargets.pop()

        job = WFLJob(parentjobguid=jobguid, actionName=actorname+"."+actionname, executionparams=executionparams)
        params['jobguid'] = job.getJobGUID()

        newLogTarget = WFLJobLogTarget(job)
        q.logger.addLogTarget(newLogTarget)

        try:
            self.__taskletEngine.execute(params, tags=(actorname, actionname), path=ActorActionTaskletPath)
        except:
            q.logger.logTargets.remove(newLogTarget)
            job.raiseError("Exception occurred.")
            if oldLogTarget:
                q.logger.addLogTarget(oldLogTarget)
            raise
        else:
            q.logger.logTargets.remove(newLogTarget)
            job.done()
            if oldLogTarget:
                q.logger.addLogTarget(oldLogTarget)

            return {'result':params.get('result'), 'jobguid':job.getJobGUID()}


    def startRootobjectAction(self, rootobjectname, actionname, params, jobguid=None, executionparams={}):
        """
        Starts a given RootObject Action. Uses the tasklet engine to run the matching tasklets.
        The action will be executed in a new job that, if a jobguid is provided, is the newest child of the job.
        If a job is provided, the new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg, maxduration, customerguid.
        The executionparams passed to the function will override the properties of the job (if provided).
        The guid of the job in which the action is executed, will be added to params: params['jobguid'].

        @param rootobjectname:                 Name of the rootobject of the root object action.
        @type type:                            string

        @param actionnmame:                    Name of the action to execute on the rootobject.
        @type actionname:                      string

        @param params:                         Dictionary containing all parameters required to execute this actions.
        @type params:                          dictionary

        @param jobguid:                        Guid of the parent job
        @type jobguid:                         guid

        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority
        @type executionParams:                 dictionary

        @return:                               dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary

        @raise ActionNotFoundException:        In case no actions are found for the specified rootobjectname and actoraction
        @raise e:                              In case an error occurred, exception is raised
        """
        if len(self.__taskletEngine.find(tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)) == 0:
            raise ActionNotFoundException("RootojbectAction", rootobjectname, actionname)

        oldLogTarget = isinstance(q.logger.logTargets[-1], WFLJobLogTarget) and q.logger.logTargets.pop()

        job = WFLJob(parentjobguid=jobguid, actionName=rootobjectname+"."+actionname, executionparams=executionparams)
        params['jobguid'] = job.getJobGUID()

        newLogTarget = WFLJobLogTarget(job)
        q.logger.addLogTarget(newLogTarget)

        try:
            self.__taskletEngine.execute(params, tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)
        except:
            q.logger.logTargets.remove(newLogTarget)
            job.raiseError("Exception occurred.")
            if oldLogTarget:
                q.logger.addLogTarget(oldLogTarget)
            raise
        else:
            q.logger.logTargets.remove(newLogTarget)
            job.done()
            if oldLogTarget:
                q.logger.addLogTarget(oldLogTarget)

            return {'result':params.get('result'), 'jobguid':job.getJobGUID()}


