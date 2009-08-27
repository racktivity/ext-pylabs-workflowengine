from pymonkey.log.LogTargets import LogTarget

class WFLJobLogTarget(LogTarget):
    def __init__(self, job, maxVerbosityLevel=5):
        LogTarget.__init__(self)
        self.job = job
        self.maxVerbosityLevel = maxVerbosityLevel

    def __str__(self):
        return "WFLJobLogTarget logging for job :%s"%(self.job.getJobGUID())

    def ___repr__(self):
        return str(self)

    def log(self, record):
        self.job.log(record.msg, record.verbosityLevel)
