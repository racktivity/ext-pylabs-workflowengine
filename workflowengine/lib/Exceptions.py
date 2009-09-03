# IMPORTANT REMARK: in order to get yaml to dump the exceptions, all parameters on the constructor have to be OPTIONAL ! 

class WFLException(Exception):
    def __init__(self, exception_name=None, exception_message=None, stacktrace=None):
        self.exception_name = exception_name
        self.exception_message = exception_message
        self.stacktrace = stacktrace
    
    def __str__(self):
        return "Exception: %s\nMessage: %s \nStacktrace:\n%s\n" % (self.exception_name, self.exception_message, self.stacktrace)

class JobNotRunningException(Exception):
    def __init__(self, jobguid=None, status=None):
        self.jobguid = jobguid
        self.status = status
    
    def __str__(self):
        return "Job '" + str(self.jobguid) +"' is not running, status is " + str(self.status)


class ActionNotFoundException(Exception):
    def __init__(self, type=None, object=None, name=None):
        self.type = type
        self.object = object
        self.name = name
    
    def __str__(self):
        return str(self.type) + " not found: " + str(self.object) + "." + str(self.name)


class ScriptFailedException(Exception):
    def __init__(self, jobguid=None, agentguid=None, scriptpath=None, errorcode=None, errormessage=None):
        self.jobguid = jobguid
        self.agentguid = agentguid
        self.scriptpath = scriptpath
        self.errorcode = errorcode
        self.errormessage = errormessage
    
    def __str__(self):
        return "Script '" + str(self.scriptpath) + "' on agent '" + str(self.agentguid) + "' for job '" + str(self.jobguid) + "' failed with errorcode " + str(self.errorcode) + " : " + str(self.errormessage) 


class TimeOutException(Exception):
    def __init__(self, jobguid=None, agentguid=None, scriptpath=None,timeout=None):
        self.jobguid = jobguid
        self.agentguid = agentguid
        self.scriptpath = scriptpath
        self.timeout = timeout
    
    def __str__(self):
        return "Script '" + str(self.scriptpath) + "' on agent '" + str(self.agentguid) + "' for job '" + str(self.jobguid) + "' timed out. Script took longer than "+ str(self.timeout) + " seconds."
        

class AgentNotAvailableException(Exception):
    def __init__(self, agentguid=None):
        self.agentguid = agentguid
    
    def __str__(self):
        return "Agent '" + str(self.agentguid) + "' is not available."

