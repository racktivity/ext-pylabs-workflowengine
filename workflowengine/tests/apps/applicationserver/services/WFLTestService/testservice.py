from pymonkey import q


class WFLTestService:

    @q.manage.applicationserver.expose
    def test(self, num):
        ret = q.workflowengine.actionmanager.startRootobjectAction("ro_test", "test", {'num':num}, executionparams={'maxduration':10})

        params = ret['result']
        jobguid = ret['jobguid']
        return "Job " + jobguid + ": test successful : " + str(params)

    @q.manage.applicationserver.expose
    def rectest(self, x, y, serialize=True, wait_in_sec=0, executionparams=None):

        params = { 'x': x,
                   'y': y,
                   'serialize': serialize,
                   'wait_in_sec': wait_in_sec,
        }

        return q.workflowengine.actionmanager.startRootobjectAction("ro_test", "rectest", params, executionparams=executionparams)

