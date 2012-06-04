__author__ = 'incubaid'
__priority__= 3

from osis.store import OsisConnection

def main(q, i, p, params, tags):
    sql= '''SELECT 
    JOBLIST.guid, 
    JOBLIST.parentjobguid, 
    JOBLIST.actionname, 
    JOBLIST.description, 
    JOBLIST.name ,
    JOBLIST.description , 
    JOBLIST.starttime, 
    JOBLIST.endtime, 
    JOBLIST.jobstatus
FROM %(table)s JOBLIST
WHERE JOBLIST.jobStatus in (%(jobstatus)s) and JOBLIST.parentjobguid IS NULL
AND (JOBLIST.rootobjecttype IS NULL OR JOBLIST.rootobjecttype NOT IN ('job','cmc'))
ORDER BY JOBLIST.starttime DESC LIMIT %(maxrows)s'''

    jobStatus = "'RUNNING'"
    table = OsisConnection.getTable(domain = 'core', objType = 'job')
    if params['errorsonly'] == True:
        jobStatus = "'ERROR'"

    params['result'] = p.api.model.core.job.query(sql % {'jobstatus' : jobStatus, 
                                                         'maxrows'   : params['maxrows'],
                                                         'table'     : table})


def match(q, i, params, tags):
    return True