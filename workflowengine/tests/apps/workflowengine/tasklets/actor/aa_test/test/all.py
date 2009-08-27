__tags__ = 'aa_test', 'test'
__author__ = 'fred'

def main(q, i, params, tags):
    params['resultaa'] = 21
    
    q.logger.log("Calling the agent... Lets hope it works...", 3)
    ret = q.workflowengine.agentcontroller.executeActorActionScript('agent1', 'aa_test', 'test', 'test_agent', {'input':'hello'}, params['jobguid'])    
    params['resultagent'] = ret['result']['agent_output']

def match(q,i,params,tags):
    return True
