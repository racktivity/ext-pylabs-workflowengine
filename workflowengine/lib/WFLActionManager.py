from pymonkey import q
from pymonkey.tasklets import TaskletsEngine

from workflowengine.WFLJob import WFLJob
from workflowengine.Exceptions import ActionNotFoundException, WFLException

from concurrence import Tasklet, Message

ActionManagerTaskletPath = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','tasklets')
ActorActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'actor') 
RootobjectActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'rootobject')

class MSG_ACTION_CALL(Message): pass
class MSG_ACTION_NOWAIT(Message): pass
class MSG_ACTION_RETURN(Message): pass
class MSG_ACTION_EXCEPTION(Message): pass

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
        self.__taskletEngine.addFromPath(ActorActionTaskletPath)
        self.__taskletEngine.addFromPath(RootobjectActionTaskletPath)
    
    def init(self):
        '''
        Does nothing: used to force pymonkey to create the q-tree mapping and initialize the taskletengine.
        '''
        pass
    
    def startActorAction(self, actorname, actionname, params, executionparams={}, jobguid=None):
        """
        Starts a given Actor Action. Uses the tasklet engine to run the matching tasklets.
        
        The current job will be determined automatically, it can however be overriden by setting jobguid.      
        The action will be executed in a new job that is the newest child of the current job, the new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg and maxduration.
        
        The executionparams passed to the function will override the properties of the job (if provided).

        The guid of the job in which the action is executed, will be added to params: params['jobguid'].
        The tasklets executing in the Actor Action have to add their result in params['result']. The dictionary returned by startActorAction contains this result, under the 'result' key.
        
        @param actorname:                      Name of the actor of the actor action.
        @type actorname:                       string
        
        @param actionnmame:                    Name of the action of the actor action.
        @type actionname:                      string
        
        @param params:                         Dictionary containing all parameters required to execute this actions. 
        @type params:                          dictionary
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority 
        @type executionParams:                 dictionary
        
        @param jobguid:                        Optional parameter, can be used to override the current job.
        @type jobguid:                         guid

        @return:                               Dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise ActionNotFoundException:        In case no actions are found for the specified actorname and actoraction
        @raise e:                              In case an error occurred, exception is raised
        """
        if len(self.__taskletEngine.find(tags=(actorname, actionname), path=ActorActionTaskletPath)) == 0:
            raise ActionNotFoundException("ActorAction", actorname, actionname)
        #SETUP THE JOB AND THE PARAMS
        currentjob = (jobguid and q.drp.job.get(jobguid)) or Tasklet.current().job.job
        job = WFLJob(parentjob=currentjob, actionName=actorname+"."+actionname, executionparams=executionparams) 
        params['jobguid'] = job.getJobGUID()
        #START A NEW TASKLET FOR THE JOB
        tasklet = Tasklet.new(self.__execute)(Tasklet.current(), job, params, (actorname, actionname), ActorActionTaskletPath)
        #WAIT FOR THE ANSWER
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_ACTION_NOWAIT):
            return { 'jobguid':job.getJobGUID(), 'result':None }
        if msg.match(MSG_ACTION_RETURN):
            return { 'jobguid':job.getJobGUID(), 'result':params.get('result')}
        elif msg.match(MSG_ACTION_EXCEPTION):
            raise args[0]
        
    def startRootobjectAction(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        """        
        Starts a given RootObject Action. Uses the tasklet engine to run the matching tasklets.
        
        The current job will be determined automatically, it can however be overriden by setting jobguid.      
        The action will be executed in a new job that is the newest child of the current job, the new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg and maxduration.
        
        The executionparams passed to the function will override the properties of the job (if provided).
        .
        The guid of the job in which the action is executed, will be added to params: params['jobguid'].
        The tasklets executing in the Actor Action have to add their result in params['result']. The dictionary returned by startActorAction contains this result, under the 'result' key.
        
        @param rootobjectname:                 Name of the rootobject of the root object action.
        @type type:                            string
        
        @param actionnmame:                    Name of the action to execute on the rootobject.
        @type actionname:                      string
        
        @param params:                         Dictionary containing all parameters required to execute this actions. 
        @type params:                          dictionary
        
        @param executionParams:                Dictionary can following keys: name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority, clouduserguid, rootobjecttype, rootobjectguid 
        @type executionParams:                 dictionary
        
        @param jobguid:                        Optional parameter, can be used to override the current job.
        @type jobguid:                         guid
        
        @return:                               dictionary with action result as result and the action's jobguid: {'result': <result>, 'jobguid': guid}
        @rtype:                                dictionary
        
        @raise ActionNotFoundException:        In case no actions are found for the specified rootobjectname and actoraction
        @raise e:                              In case an error occurred, exception is raised
        """
        if len(self.__taskletEngine.find(tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)) == 0:
            raise ActionNotFoundException("RootobjectAction", rootobjectname, actionname)
        #SETUP THE JOB AND THE PARAMS
        currentjob = (jobguid and q.drp.job.get(jobguid)) or ((hasattr(Tasklet.current(), 'job') and getattr(Tasklet.current(), 'job').job) or None)
        job = WFLJob(parentjob=currentjob, actionName=rootobjectname+"."+actionname, executionparams=executionparams)
        params['jobguid'] = job.getJobGUID()
        #START A NEW TASKLET FOR THE JOB
        tasklet = Tasklet.new(self.__execute)(Tasklet.current(), job, params, (rootobjectname, actionname), RootobjectActionTaskletPath)
        #WAIT FOR THE ANSWER
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_ACTION_NOWAIT):
            return { 'jobguid':job.getJobGUID(), 'result':None }
        if msg.match(MSG_ACTION_RETURN):
            return { 'jobguid':job.getJobGUID(), 'result':params.get('result')}
        elif msg.match(MSG_ACTION_EXCEPTION):
            raise args[0]


    def __execute(self, parentTasklet, job, params, tags, path):
        #SETUP THE CONTEXT
        Tasklet.current().job = job
        Tasklet.current().tags = tags
        
        if job.wait is False: MSG_ACTION_NOWAIT.send(parentTasklet)()
        
        #EXECUTE THE TASKLETS
        try:
            self.__taskletEngine.execute(params, tags=tags, path=path)
        except Exception, e:
            job.raiseError(e)
            if job.wait is True: MSG_ACTION_EXCEPTION.send(parentTasklet)(WFLException.create(e))
        else:
            job.done()
            if job.wait is True: MSG_ACTION_RETURN.send(parentTasklet)()
