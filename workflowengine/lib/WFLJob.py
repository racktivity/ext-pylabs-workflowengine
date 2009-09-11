from datetime import datetime
from time import mktime

from workflowengine.DRPClient import DRPClient
from workflowengine.Exceptions import ParentNotFoundException
from workflowengine.Exceptions import JobNotRunningException
from workflowengine.Exceptions import LogmessageFormatViolationException

def inheritFromParent(childjob, parentjob):
    for field in ['name', 'description', 'userErrormsg', 'internalErrormsg', 'maxduration', 'customerguid']:
        setattr(childjob, field, getattr(parentjob, field))

def fillInExecutionparams(job, executionparams={}):
    for field in ['name', 'description', 'userErrormsg', 'internalErrormsg', 'maxduration', 'customerguid']:
        if executionparams.has_key(field):
            setattr(job, field, executionparams[field])

def getUnixTimestamp():
    t = datetime.now()
    return int(mktime(t.timetuple())+1e-6*t.microsecond)

def createLogEntry(epoch, level, source, logmessage):
    return str(epoch) + "|" + str(level) + "|" + source + "|" + logmessage + "\n"

class WFLJob:

    def __init__(self, parentjobguid=None, actionName="", executionparams={}, agentguid=""):
        """
        Creates a new job in the DRP and returns an instance of WFLJob. The WFLJob contains a disconnected job object and adds extra functionality.
        
        @param parentjobguid:     The guid of the parent job, can be None. If a parent is provided, the new job will inherit (name, description, userErrormsg, internalErrormsg, maxduration, customerguid) of the parent.
        @param executionparams:   Dictorionary that can contain keys : name, description, userErrormsg, internalErrormsg, maxduration, customerguid, wait, timetostart, priority.
        
        name, description, userErrormsg, internalErrormsg, maxduration and customerguid are attributes of core.jobs (see DRP model)
        wait:              If True, the caller of the action executed in the job will be blocked until the job finished. If false: the job will be scheduled in the actionqueue. V1: Only True is available.
        timetostart:       If wait==True: the time at which the job should start, if None the job will be scheduled as soon as possible. V1: not available.
        priority:          If wait==True: the priority of the job in the scheduler; the lower the number, the higher the priority: jobs with priority 1 will be scheduled first, then jobs with priority 2 and so on. V1: not available.
        """
        self.__drp = DRPClient()
        self.__job = self.__drp.job.new()
        
        if parentjobguid:
            try:
                parentjob = self.__drp.job.get(parentjobguid)
            except:
                raise ParentNotFoundException("Parent with GUID: '" + str(parentjobguid) + "' not found.")
            else:
                self.__job.parentjobguid = parentjobguid
                self.__job.order = self.__drp.job.getNextChildOrder(parentjobguid)
                inheritFromParent(self.__job, parentjob)
        
        self.__job.actionName = actionName
        fillInExecutionparams(self.__job, executionparams)
        self.__job.agentguid = agentguid
        
        self.wait = executionparams.get('wait') and executionparams['wait']
        self.timetostart = executionparams.get('timetostart') and executionparams['timetostart']
        self.priority = executionparams.get('priority') and executionparams['priority']
        
        self.__start()
        self.__drp.job.save(self.__job)
    
    def __start(self):
        self.__job.jobstatus = "RUNNING"
        self.__job.starttime = datetime.now()
        self.__job.log = ""

    def getJobGUID(self):
        """
        Get the guid of the job.
        @return: jobguid
        """
        return self.__job.guid

    def getMaxduration(self):
        """
        Get the maxduration of the job.
        @return: maxduration
        """
        return self.__job.maxduration

    def log(self, logmessage, level=5, source=""):
        """
        Creates a new log entry in the job. Internal format is specified in the DRP model.
        None of the parameters can contain the "|" character, an exception will be thrown if this constraint is violated.
        
        @param logmessage:        The message to log.
        @param level:             The loglevel: TODO add debug levels from pylabs, couldn't find them...
        @param source:            The source of the log message, eg: an agent.
        
        @raise LogmessageFormatViolationException: if the logmessage contains '|' 
        """
        if '|' in logmessage:
            raise LogmessageFormatViolationException("Logmessage: '" + logmessage + "' contains invalid symbol '|'")
        
        epoch = getUnixTimestamp()
        logentry = createLogEntry(epoch, level, source, logmessage)
        self.__job.log = self.__job.log + logentry
        #self.__drp.job.save(self.__job)
    
    
    def done(self):
        """
        Should be called if the job has finished: sets the status to DONE, sets the timing information and commits the job to the DRP. 
        
        @raise JobNotRunningException: if the job is not running
        """
        if str(self.__job.jobstatus) <> "RUNNING":
            raise JobNotRunningException("Job '" + self.__job.guid + "' is not running: state is " + str(self.__job.jobstatus))
        
        self.__job.jobstatus = "DONE"
        self.__job.endtime = datetime.now()
        self.__drp.job.save(self.__job)
    
    def raiseError(self, message):
        """
        Should be called if the job has failed: it adds a message to the log and sets the proper status.
        
        @param message: the message to log.
        @raise JobNotRunningException: if the job is not running
        """
        if str(self.__job.jobstatus) <> "RUNNING":
            raise JobNotRunningException("Job '" + self.__job.guid + "' is not running: state is " + str(self.__job.jobstatus))
        
        self.__job.jobstatus = "ERROR"
        self.__job.endtime = datetime.now()
        self.log(message, 1, "WFLJob.raiseError")
        self.__drp.job.save(self.__job)
        
    def commit(self):
        """
        Commits the job to the DRP.
        """
        self.__drp.job.save(self.__job)

