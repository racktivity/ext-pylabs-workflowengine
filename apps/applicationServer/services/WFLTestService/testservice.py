from pymonkey import q, i


class WFLTestService:
    def __init__(self):
        pass
    
    @q.manage.applicationserver.expose
    def test(self, num):
        ret = q.workflowengine.actionmanager.startRootobjectAction("ro_test", "test", {'num':num})
        params = ret['result']
        jobguid = ret ['jobguid']
        return "Job " + jobguid + ": test successful : " + str(params)

    @q.manage.applicationserver.expose
    def agents(self):
        return str(q.workflowengine.agentcontroller.agents())
