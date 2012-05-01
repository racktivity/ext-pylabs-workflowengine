from pylabs import q

from workflowengine.Exceptions import ActionNotFoundException, WFLException
from workflowengine.WFLJobManager import MSG_JOB_FINISHED

from workflowengine import getAppName

from concurrence import Tasklet, Message

#ActionManagerTaskletPath = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','tasklets')
#ActorActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'actor')
#RootobjectActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'rootobject')
ActorActionTaskletPath = q.system.fs.joinPaths(q.dirs.baseDir, 'pyapps',
        getAppName(), 'impl', 'actor')
RootobjectActionTaskletPath = q.system.fs.joinPaths(q.dirs.baseDir, 'pyapps',
        getAppName(), 'impl', 'action')

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
        ##create tasklets dir if it doesnt exist
        if not q.system.fs.exists(ActorActionTaskletPath):
            q.system.fs.createDir(ActorActionTaskletPath)
        if not q.system.fs.exists(RootobjectActionTaskletPath):
            q.system.fs.createDir(RootobjectActionTaskletPath)

        self.__taskletEngine = q.taskletengine.get(ActorActionTaskletPath)
        self.__taskletEngine.addFromPath(RootobjectActionTaskletPath)

    def init(self):
        '''
        Does nothing: used to force pylabs to create the q-tree mapping and initialize the taskletengine.
        '''
        pass

    def startActorAction(self, domainname, actorname, actionname, params, executionparams={}, jobguid=None):
        """
        Starts a given Actor Action. Uses the tasklet engine to run the matching tasklets.

        The current job will be determined automatically, it can however be overriden by setting jobguid.
        The action will be executed in a new job that is the newest child of the current job, the new job will inherit the following properties of the job: name, description, userErrormsg, internalErrormsg and maxduration.

        The executionparams passed to the function will override the properties of the job (if provided).

        The guid of the job in which the action is executed, will be added to params: params['jobguid'].
        The tasklets executing in the Actor Action have to add their result in params['result']. The dictionary returned by startActorAction contains this result, under the 'result' key.

        @param domainname:                     Name of the domain of the actor action.
        @type domainname:                      string
    
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
        if len(self.__taskletEngine.find(tags=(domainname, actorname, actionname), path=ActorActionTaskletPath)) == 0:
            raise ActionNotFoundException("ActorAction", domainname, actorname, actionname)
        #SETUP THE JOB AND THE PARAMS
        currentjobguid = jobguid or Tasklet.current().jobguid
        if q.workflowengine.jobmanager.isKilled(currentjobguid):
           raise Exception("Can't create child jobs: the job is killed !")
        
        params['jobguid'] = jobguid = q.workflowengine.jobmanager.createJob(currentjobguid, domainname+"."+actorname+"."+actionname, executionparams, params=params)
        #START A NEW TASKLET FOR THE JOB

        q.workflowengine.jobmanager.startJob(jobguid)
        Tasklet.new(self.__execute)(Tasklet.current(), jobguid, params, (domainname, actorname, actionname), ActorActionTaskletPath)
        #WAIT FOR THE ANSWER
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_ACTION_NOWAIT):
            return { 'jobguid':jobguid, 'result':None }
        if msg.match(MSG_ACTION_RETURN):
            return { 'jobguid':jobguid, 'result':params.get('result')}
        elif msg.match(MSG_ACTION_EXCEPTION):
            raise args[0]

    def startRootobjectAction(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):
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
        path = RootobjectActionTaskletPath

        if len(self.__taskletEngine.find(tags=(domainname, rootobjectname, actionname), path=path)) == 0:
            raise ActionNotFoundException("RootobjectAction", domainname, rootobjectname, actionname)
        #SETUP THE JOB AND THE PARAMS
        currentjobguid = jobguid or (hasattr(Tasklet.current(), 'jobguid') and Tasklet.current().jobguid) or None
        if executionparams.get('rootjob'):
            currentjobguid = None
        if q.workflowengine.jobmanager.isKilled(currentjobguid):
           raise Exception("Can't create child jobs: the job is killed !")
        
        params['jobguid'] = jobguid = q.workflowengine.jobmanager.createJob(currentjobguid, domainname + '.' + rootobjectname+"."+actionname, executionparams, params=params)
        #START A NEW TASKLET FOR THE JOB
        q.workflowengine.jobmanager.startJob(jobguid)

        Tasklet.new(self.__execute)(Tasklet.current(), jobguid, params, (domainname, rootobjectname, actionname), path)

        #WAIT FOR THE ANSWER
        (msg, args, kwargs) = Tasklet.receive().next()

        if msg.match(MSG_ACTION_NOWAIT):
            return { 'jobguid':jobguid, 'result':None }
        if msg.match(MSG_ACTION_RETURN):
            return { 'jobguid':jobguid, 'result':params.get('result')}
        elif msg.match(MSG_ACTION_EXCEPTION):
            raise args[0]

    def startRootobjectActionAsynchronous(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        """
        API compatibility with CloudAPIActionManager
        """
        return self.startRootobjectAction(domainname, rootobjectname, actionname, params, executionparams, jobguid)

    def startRootobjectActionSynchronous(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        """
        API compatibility with CloudAPIActionManager
        """
        return self.startRootobjectAction(domainname, rootobjectname, actionname, params, executionparams, jobguid)

    def waitForActions(self, jobguids):
        """
        Wait for some background jobs to finish.

        @param jobguids:  A list containing the jobguids of the jobs you want to wait for.
        @type jobguids:   array of jobguids
        """
        for jobguid in jobguids:
            q.workflowengine.jobmanager.registerJobFinishedCallback(jobguid)

        out = {}
        while len(jobguids) > 0:
            (msg, args, kwargs) = Tasklet.receive().next()
            if msg.match(MSG_JOB_FINISHED):
                (jobguid, status, result) = args
                jobguids.remove(jobguid)
                out[jobguid] = (status, result)

        failed = filter(lambda jobguid: out[jobguid][0] == 'ERROR', out)

        if failed:
            raise WFLException.createCombo(map(lambda jobguid: out[jobguid][1], failed))
        else:
            return dict(zip(map(lambda jobguid: jobguid, out), map(lambda jobguid: out[jobguid][1], out)))


    def __execute(self, parentTasklet, jobguid, params, tags, path):
        #SETUP THE CONTEXT
        Tasklet.current().jobguid = jobguid
        wait = q.workflowengine.jobmanager.shouldWait(jobguid)
        Tasklet.current().tags = tags

        if wait is False: MSG_ACTION_NOWAIT.send(parentTasklet)()

        #EXECUTE THE TASKLETS
        try:
            self.__taskletEngine.execute(params, tags=tags, path=path)
        except Exception, e:
            q.workflowengine.jobmanager.setJobDied(jobguid, e)
            if wait is True: MSG_ACTION_EXCEPTION.send(parentTasklet)(WFLException.create(e,jobguid))

        else:
            q.workflowengine.jobmanager.setJobDone(jobguid, params.get('result'))
            if wait is True: MSG_ACTION_RETURN.send(parentTasklet)()
