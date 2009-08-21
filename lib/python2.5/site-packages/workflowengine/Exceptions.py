class ParentNotFoundException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)

class JobNotRunningException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        
class LogmessageFormatViolationException(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
        
class ActionNotFoundException(Exception):
    def __init__(self, type, object, name):
        Exception.__init__(self, type+" not found: "+object+"."+name)

class ScriptFailedException(Exception):
    def __init__(self, errorcode, errormessage):
        Exception.__init__(self, "errorcode=" + str(errorcode) +", " + errormessage)
        self.errorcode = errorcode
        self.errormessage = errormessage

class JobFailedException(Exception):
    def __init__(self, exception, jobguid):
        Exception.__init__(self, "Job " + jobguid + " : " + str(exception))
        self.exception = exception
        self.jobguid = jobguid
