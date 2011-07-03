__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    
    for jobguid in params['jobguids']:
        q.logger.log('Deleting job %s '% jobguid,3)
        p.api.model.core.job.delete(jobguid)
    
    params['result'] = True

def match(q, i, params, tags):
    return True
