__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    q.logger.log('Creating the new job in the model', 3)
    job = p.api.model.core.job.new()
    p.api.model.core.job.save(job)
    params['result'] = job.guid

def match(q, i, params, tags):
    return True
