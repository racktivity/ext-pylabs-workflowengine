__tags__ = 'ro_test', 'test'
__author__ = 'fred'

def main(q, i, params, tags):

    q.logger.log("Num = " + str(params['num']), 3)
    
    ret = q.workflowengine.actionmanager.startActorAction("aa_test", "test", {})
    params['result'] = ret['result']

def match(q,i,params,tags):
    return params['num'] == 0
