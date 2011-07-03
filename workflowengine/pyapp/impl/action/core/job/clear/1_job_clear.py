__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    
    p.api.model.core.job.query("DELETE FROM core_job.core_view_job_list;SELECT True")
    p.api.model.core.job.query("DELETE FROM core_job.core_view_job_parentlist;SELECT True")
    
    params['result'] = True
    
def match(q, i, params, tags):
    return True
