__author__ = 'incubaid'
__priority__= 3

from osis.store import OsisConnection

def main(q, i, p, params, tags):
    jobScheme = OsisConnection.getSchemeName(domain = 'core', objType = 'job')
    jobTable = OsisConnection.getTableName(domain = 'core', objType = 'job')
    p.api.model.core.job.query("DELETE FROM %s.%s;SELECT True" % (jobScheme, jobTable))
    p.api.model.core.job.query("DELETE FROM %s.job_parentlist;SELECT True" % jobScheme)
    
    params['result'] = True
    
def match(q, i, params, tags):
    return True
