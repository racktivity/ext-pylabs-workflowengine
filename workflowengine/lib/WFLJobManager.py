from pymonkey import q

from datetime import datetime
from time import mktime

from workflowengine.Exceptions import JobNotRunningException, WFLException

from concurrence import Tasklet, Message
class MSG_JOB_FINISHED(Message): pass

class WFLJobManager:

    def __init__(self):
        self.__waitingJobs = {}
        self.__runningJobs = {}
        self.__stoppedJobs = {}

        self.__rootJobGuid_treejobs_mapping = {}

    def createJob(self, parentjobguid, actionName, executionparams, agentguid=None, params=""):
        parentjob = parentjobguid and self.__getJob(parentjobguid)
        job = WFLJob(parentjob, actionName, executionparams, agentguid, params)
        # The ancestor (top of the jobtree) is used to perform refcount garbage collection on the stoppedJobs
        # We are certain no joins are possible when all jobs within a job tree are stopped.
        # The number of running (= non stopped) jobs is stored in the ancestor.
        if job.isRootJob(): self.__rootJobGuid_treejobs_mapping[job.drp_object.guid] = [job]
        else: self.__rootJobGuid_treejobs_mapping[job.ancestor.drp_object.guid].append(job)
        job.ancestor.runningJobsInTree += 1
        # TODO Waiting jobs should be stored in the action queue in OSIS
        self.__waitingJobs[job.drp_object.guid] = job
        return job.drp_object.guid

    def startJob(self, jobguid):
        if jobguid in self.__waitingJobs:
            job = self.__waitingJobs.pop(jobguid)
            self.__runningJobs[jobguid] = job
            job.start()
            # TODO Running jobs should be stored in the action queue in OSIS
        else:
            # TODO Check in OSIS if the job exists and if it is waiting: should be stored in the action queue
            raise Exception("Job '%s' cannot be started, it is not waiting." % jobguid)

    def setJobDone(self, jobguid, result):
        if jobguid in self.__runningJobs:
            job = self.__runningJobs.pop(jobguid)
            self.__stoppedJobs[job.drp_object.guid] = job
            job.done(result)
            self.__stoppedJob(job)
        else:
            # TODO Check in OSIS if the job exists and if it is running: should be stored in the action queue
            raise Exception("Job '%s' cannot be stopped, it is not running." % jobguid)

    def setJobDied(self, jobguid, exception):
        if jobguid in self.__runningJobs:
            job = self.__runningJobs.pop(jobguid)
            self.__stoppedJobs[job.drp_object.guid] = job
            job.died(exception)
            self.__stoppedJob(job)
        else:
            # TODO Check in OSIS if the job exists and if it is running: should be stored in the action queue
            raise Exception("Job '%s' cannot be stopped, it is not running." % jobguid)

    def __stoppedJob(self, job):
        self.__notifyJobFinishedCallbacks(job)

        # Do garbage collection if the jobtree is empty !
        job.ancestor.runningJobsInTree -= 1
        if job.ancestor.runningJobsInTree is 0:
             jobs = self.__rootJobGuid_treejobs_mapping.pop(job.ancestor.drp_object.guid)
             for job in jobs:
                 self.__stoppedJobs.pop(job.drp_object.guid)

    def appendJobLog(self, jobguid, logmessage, level=5, source=""):
        if jobguid in self.__runningJobs:
            self.__runningJobs[jobguid].log(logmessage, level, source)
        else:
            # TODO Check in OSIS if the job exists and if it is running: should be stored in the action queue
            raise Exception("Job '%s' is not started." % jobguid)

    def shouldWait(self, jobguid):
        job = self.__getJob(jobguid)
        return job.wait

    def getMaxduration(self, jobguid):
        job = self.__getJob(jobguid)
        return job.drp_object.maxduration

    def registerJobFinishedCallback(self, jobguid):
        job = self.__getJob(jobguid)
        if job.drp_object.jobstatus is q.enumerators.jobstatus.WAITING or job.drp_object.jobstatus is q.enumerators.jobstatus.RUNNING:
            job.jobFinishedCallbacks.append(Tasklet.current())
        else:
            self.__notifyJobFinishedCallback(Tasklet.current(), job)

    def __getJob(self, jobguid):
        if jobguid in self.__waitingJobs:
            return self.__waitingJobs[jobguid]
        elif jobguid in self.__runningJobs:
            return self.__runningJobs[jobguid]
        elif jobguid in self.__stoppedJobs:
            return self.__stoppedJobs[jobguid]
        else:
            raise Exception("Could not find the job %s in waiting, running or stopped jobs" % jobguid)

    def __notifyJobFinishedCallbacks(self, job):
        for tasklet in job.jobFinishedCallbacks:
            self.__notifyJobFinishedCallback(tasklet, job)

    def __notifyJobFinishedCallback(self, tasklet, job):
        if job.drp_object.jobstatus is q.enumerators.jobstatus.DONE:
            MSG_JOB_FINISHED.send(tasklet)(job.drp_object.guid, 'DONE', job.result)
        elif job.drp_object.jobstatus is q.enumerators.jobstatus.ERROR:
            MSG_JOB_FINISHED.send(tasklet)(job.drp_object.guid, 'ERROR', job.exception)


def inheritFromParent(childjob, parentjob):
    for field in ['name', 'description', 'userErrormsg', 'internalErrormsg', 'maxduration']:
        setattr(childjob, field, getattr(parentjob, field))

def fillInExecutionparams(job, executionparams={}):
    for field in ['name', 'description', 'userErrormsg', 'internalErrormsg', 'maxduration', 'clouduserguid', 'rootobjecttype', 'rootobjectguid']:
        if executionparams.has_key(field):
            setattr(job, field, executionparams[field])

def getUnixTimestamp():
    t = datetime.now()
    return int(mktime(t.timetuple())+1e-6*t.microsecond)

class WFLJob:

    def __init__(self, parentjob=None, actionName=None, executionparams={}, agentguid=None, params=""):
        """
        Creates a new job in the DRP and returns an instance of WFLJob. The WFLJob contains a disconnected job object and adds extra functionality.

        @param parentjob:     The parent job, can be None. If a parent is provided, the new job will inherit (name, description, userErrormsg, internalErrormsg, maxduration) of the parent.
        @type parentjob:      WFLJob

        @param executionparams:   Dictorionary that can contain keys : name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority, clouduserguid, rootobjecttype, rootobjectguid.
        @type executionparams:    dict

        name, description, userErrormsg, internalErrormsg and maxduration are attributes of core.jobs (see DRP model)
        wait:              If True, the caller of the action executed in the job will be blocked until the job finished. If false: the job will be scheduled in the actionqueue.
        timetostart:       If wait==True: the time at which the job should start, if None the job will be scheduled as soon as possible. V1: not available.
        priority:          If wait==True: the priority of the job in the scheduler; the lower the number, the higher the priority: jobs with priority 1 will be scheduled first, then jobs with priority 2 and so on. V1: not available.
        """
        self.create_drp_object()
        self.ancestor = self
        self.runningJobsInTree = 0

        if parentjob:
            self.drp_object.parentjobguid = parentjob.drp_object.guid
            self.drp_object.order = WFLJob.getNextChildOrder(parentjob.drp_object.guid)
            #inheritFromParent(self.drp_object, parentjob.drp_object)
            self.ancestor = parentjob.ancestor

        self.drp_object.actionName = actionName
        self.drp_object.agentguid = agentguid
        self.drp_object.params = params

        fillInExecutionparams(self.drp_object, executionparams)
        # TODO Should the below parameters be stored in OSIS ? -> required for delayed jobs !
        self.wait = True if executionparams.get('wait') is None else executionparams.get('wait')
        self.timetostart = executionparams.get('timetostart')
        self.priority = executionparams.get('priority')
        self.jobFinishedCallbacks = []

        self.commit_drp_object()

    def isRootJob(self):
        # If the job has no parent, it was called through the cloud API => it's a root job !
        # TODO: THIS WILL BREAK IF THE CLOUD API IS CALLED WITH A PARENTJOBGUID !!! -> MEMORY LEAK !
        return self.ancestor is self

    def start(self):
        self.drp_object.jobstatus = q.enumerators.jobstatus.RUNNING
        self.drp_object.starttime = datetime.now()
        self.drp_object.log = ""
        self.commit_drp_object()

    def log(self, logmessage, level, source):
        """
        Creates a new log entry in the job. Internal format is specified in the DRP model.
        The parameters may not contain '|', if they do, '|' will be replaced by '/'

        @param logmessage:        The message to log.
        @param level:             The loglevel, the maximum loglevel is 5.
        @param source:            The source of the log message, eg: an agent.
        """
        if '|' in logmessage: logmessage = logmessage.replace('|', '/')
        if '\n' in logmessage: logmessage = logmessage.replace('\n', ' ')

        if '|' in source: source = source.replace('|', '/')
        if '\n' in source: source = source.replace('\n', ' ')

        logentry = str(getUnixTimestamp()) + "|" + str(level) + "|" + source + "|" + logmessage + "\n"
        self.drp_object.log = self.drp_object.log or "" + logentry
        # TODO Logs should be saved in OSIS instantaneously, not implemented for performance reasons. Should have buffering -> store every second ? Start tasklet with sleep to do this.

    def done(self, result):
        """
        Should be called if the job has finished: sets the status to q.enumerators.jobstatus.DONE, sets the timing information and commits the job to the DRP.

        @raise JobNotRunningException: if the job is not running
        """
        if self.drp_object.jobstatus <> q.enumerators.jobstatus.RUNNING:
            raise JobNotRunningException(self.drp_object.guid, self.drp_object.jobstatus)

        self.drp_object.jobstatus = q.enumerators.jobstatus.DONE
        self.drp_object.endtime = datetime.now()
        self.result = result
        self.commit_drp_object()

    def died(self, exception):
        """
        Should be called if the job has failed: it adds a message to the log and sets the proper status.

        @param exception: the exception to log.
        @raise JobNotRunningException: if the job is not running
        """
        if self.drp_object.jobstatus <> q.enumerators.jobstatus.RUNNING:
            raise JobNotRunningException(self.drp_object.guid, self.drp_object.jobstatus)

        self.drp_object.jobstatus = q.enumerators.jobstatus.ERROR
        self.drp_object.endtime = datetime.now()
        if not isinstance(exception, WFLException):
            self.log(str(exception), 1, "Exception occured")
        self.exception = exception
        self.commit_drp_object()

    def create_drp_object(self):
        """
        Create a new job in the DRP.
        """
        self.drp_object = q.drp.job.new()
        self.drp_object.jobstatus = q.enumerators.jobstatus.WAITING

    def commit_drp_object(self):
        """
        Commits the job to the DRP.
        """
        q.drp.job.save(self.drp_object)
        self.drp_object = q.drp.job.get(self.drp_object.guid)

    @classmethod
    def findChildren(cls, parentjobguid):
        filterObj = q.drp.job.getFilterObject()
        filterObj.add('view_job_parentlist', 'parentjobguid', parentjobguid)

        childrenguids = q.drp.job.find(filterObj)
        childrenguids = set(childrenguids)

        return map(lambda x: q.drp.job.get(x), childrenguids)

    @classmethod
    def printJobTree(cls, parentjobguid, indent=0):
        job = q.drp.job.get(parentjobguid)
        print " "*indent + job.guid + " " + str(job.actionName) + " " + str(job.jobstatus) + " " + str(job.log)
        children = cls.findChildren(parentjobguid)
        for child in children:
            cls.printJobTree(child.guid, indent+1)

    @classmethod
    def getNextChildOrder(cls, parentjobguid):
        filterObj = q.drp.job.getFilterObject()
        filterObj.add('view_job_parentlist', 'parentjobguid', parentjobguid)

        view = q.drp.job.findAsView(filterObj, 'view_job_parentlist')
        highestOrder = -1
        for jobobj in view:
            if jobobj['joborder'] > highestOrder:
                highestOrder = jobobj['joborder']
        return highestOrder + 1
