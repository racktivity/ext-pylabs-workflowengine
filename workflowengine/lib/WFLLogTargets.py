from pymonkey import q

from pymonkey.log.LogTargets import LogTarget
from concurrence import Tasklet

class WFLJobLogTarget(LogTarget):

    def __init__(self):
        LogTarget.__init__(self)
        self.maxVerbosityLevel = 5

    def __str__(self):
        return "WFLJobLogTarget"

    def ___repr__(self):
        return str(self)

    def log(self, record):
        if hasattr(Tasklet.current(), 'jobguid'):
            jobguid = Tasklet.current().jobguid
            q.workflowengine.jobmanager.appendJobLog(jobguid, record.msg, record.verbosityLevel)
        else:
            # No job in the context, do nothing
            pass
