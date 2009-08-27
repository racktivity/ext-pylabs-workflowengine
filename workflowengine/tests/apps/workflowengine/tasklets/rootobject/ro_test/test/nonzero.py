__tags__ = 'ro_test', 'test'
__author__ = 'fred'

def main(q, i, params, tags):
    
    q.logger.log("Num = " + str(params['num']), 3)
    
    ret = q.workflowengine.actionmanager.startRootobjectAction("ro_test", "test", {'num':params['num']-1}, params['jobguid'])
    params['resultagent'] = ret['result']['resultagent']

def match(q,i,params,tags):
    return params['num'] <> 0