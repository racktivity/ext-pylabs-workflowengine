__tags__ = 'aa_test', 'test'
__author__ = 'fred'

def main(q, i, params, tags):
    
    q.logger.log("Calling the agent... Lets hope it works...", 3)
    ret = q.workflowengine.agentcontroller.executeActorActionScript('agent1', 'test_agent', {'input':'hello'})    
    params['result'] = ret['result']

def match(q,i,params,tags):
    return True
