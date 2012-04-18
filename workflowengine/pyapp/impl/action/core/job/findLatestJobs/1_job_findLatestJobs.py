__author__ = 'incubaid'
__priority__= 3

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
FROM core_job.core_view_job_list JOBLIST
WHERE JOBLIST.jobStatus = %(jobstatus)s and JOBLIST.parentjobguid IS NULL
AND (JOBLIST.rootobjecttype IS NULL OR JOBLIST.rootobjecttype NOT IN ('job','cmc'))
ORDER BY JOBLIST.starttime DESC LIMIT %(maxrows)s'''

    jobStatus = "RUNNING"
    if params['errorsonly'] == True:
        jobStatus = "ERROR"

    params['result'] = p.api.model.core.job.query(sql, {'jobstatus': jobStatus, 'maxrows':params['maxrows']})


def match(q, i, params, tags):
    return True
