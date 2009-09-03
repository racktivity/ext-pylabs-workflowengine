from pymonkey import q

from concurrence import Tasklet

from datetime import datetime
from time import mktime

from workflowengine.Exceptions import JobNotRunningException 

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

def createLogEntry(epoch, level, source, logmessage):
    return str(epoch) + "|" + str(level) + "|" + source + "|" + logmessage + "\n"

class WFLJob:

    def __init__(self, parentjob=None, actionName="", executionparams={}, agentguid=""):
        """
        Creates a new job in the DRP and returns an instance of WFLJob. The WFLJob contains a disconnected job object and adds extra functionality.
        
        @param parentjob:     The parent job, can be None. If a parent is provided, the new job will inherit (name, description, userErrormsg, internalErrormsg, maxduration) of the parent.
        @param executionparams:   Dictorionary that can contain keys : name, description, userErrormsg, internalErrormsg, maxduration, wait, timetostart, priority, clouduserguid, rootobjecttype, rootobjectguid.
        
        name, description, userErrormsg, internalErrormsg and maxduration are attributes of core.jobs (see DRP model)
        wait:              If True, the caller of the action executed in the job will be blocked until the job finished. If false: the job will be scheduled in the actionqueue. V1: Only True is available.
        timetostart:       If wait==True: the time at which the job should start, if None the job will be scheduled as soon as possible. V1: not available.
        priority:          If wait==True: the priority of the job in the scheduler; the lower the number, the higher the priority: jobs with priority 1 will be scheduled first, then jobs with priority 2 and so on. V1: not available.
        """
        self.job = q.drp.job.new()
        
        if parentjob:
            self.job.parentjobguid = parentjob.guid
            self.job.order = WFLJob.getNextChildOrder(parentjob.guid)
            inheritFromParent(self.job, parentjob)
        
        self.job.actionName = actionName
        self.job.agentguid = agentguid
        
        fillInExecutionparams(self.job, executionparams)
        self.wait = executionparams.get('wait')
        self.timetostart = executionparams.get('timetostart')
        self.priority = executionparams.get('priority')
        
        self.__start()
        q.drp.job.save(self.job)
    
    def __start(self):
        self.job.jobstatus = "RUNNING"
        self.job.starttime = str(datetime.now())
        self.job.log = ""

    def getJobGUID(self):
        """
        Get the guid of the job.
        @return: jobguid
        """
        return self.job.guid

    def getMaxduration(self):
        """
        Get the maxduration of the job.
        @return: maxduration
        """
        return self.job.maxduration

    def log(self, logmessage, level=5, source=""):
        """
        Creates a new log entry in the job. Internal format is specified in the DRP model.
        The parameters may not contain '|', if they do, '|' will be replaced by '/'
        
        @param logmessage:        The message to log.
        @param level:             The loglevel, the maximum loglevel is 5.
        @param source:            The source of the log message, eg: an agent.
        """
        if '|' in logmessage: logmessage = logmessage.replace('|', '/')
        if '|' in source: source = source.replace('|', '/')
        
        epoch = getUnixTimestamp()
        logentry = createLogEntry(epoch, level, source, logmessage)
        self.job.log = self.job.log + logentry
    
    def done(self):
        """
        Should be called if the job has finished: sets the status to DONE, sets the timing information and commits the job to the DRP. 
        
        @raise JobNotRunningException: if the job is not running
        """
        if str(self.job.jobstatus) <> "RUNNING":
            raise JobNotRunningException(self.job.guid, self.job.jobstatus)
        
        self.job.jobstatus = "DONE"
        self.job.endtime = str(datetime.now())
        q.drp.job.save(self.job)
    
    def raiseError(self, exception):
        """
        Should be called if the job has failed: it adds a message to the log and sets the proper status.
        
        @param exception: the exception to log.
        @raise JobNotRunningException: if the job is not running
        """
        if str(self.job.jobstatus) <> "RUNNING":
            raise JobNotRunningException(self.job.guid, self.job.jobstatus)
        
        self.job.jobstatus = "ERROR"
        self.job.endtime = str(datetime.now())
        self.log(str(exception), 1, "Exception occured")
        q.drp.job.save(self.job)
        
    def commit(self):
        """
        Commits the job to the DRP.
        """
        q.drp.job.save(self.job)

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

