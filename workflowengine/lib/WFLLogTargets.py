from pymonkey import q

from concurrence import Tasklet

class WFLJobLogTarget(object):

    def __init__(self):
        self.maxlevel = 5
        self.enabled = True

    def checkTarget(self):
        """
        check status of target, if ok return True
        for std out always True
        """
        True

    def __str__(self):
        return "WFLJobLogTarget"

    def ___repr__(self):
        return str(self)

    def log(self, message):
        if hasattr(Tasklet.current(), 'jobguid'):
            jobguid = Tasklet.current().jobguid
            q.workflowengine.jobmanager.appendJobLog(jobguid, message)
        else:
            # No job in the context, do nothing
            pass
        return True

    def __eq__(self, other):
        if not other:
            return False
        if not isinstance(other, WFLJobLogTarget):
            return False

        return True

    def close(self):
        pass

