__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    # rootobjecttype: machine, application, datacenter
    # fromTime/endTime: YYYY-MM-DD hh:mm:ss
    baseQuery = 'select guid as jobguid, clouduserguid, rootobjectguid, description, parentjobguid, viewguid,\
                 jobstatus, "version", rootobjecttype, actionname, agentguid, starttime, endtime, "name" from core_job.core_view_job_list'
    conditionQuery = list()
    filters = {'view_job_list': ['name', 'actionname', 'description', 'jobstatus', 'agentguid', 'clouduserguid',
                                 'applicationguid', 'machineguid', 'datacenterguid', 'fromTime', 'toTime', 'parentjobguid']}
    filterOptions = {'fromTime' :   ['starttime','>= '],
                     'toTime'   :   ['endtime','<= ']}
    roMapping = {'applicationguid':'application','machineguid':'machine','datacenterguid':'datacenter'}
    for filterName,filterKeys in filters.iteritems():
        for filterType in filterKeys:
            if filterType in params and params[filterType] not in ('',0):
                if filterType in roMapping:
                    conditionQuery.append("rootobjectguid = '%s'"%params[filterType])
                    conditionQuery.append("rootobjecttype = '%s'"%roMapping[filterType])
                else:
                    conditionQuery.append("%s %s '%s'"%(filterType if filterType not in filterOptions else filterOptions[filterType][0],
                                                        '=' if filterType not in filterOptions else filterOptions[filterType][1],
                                                        params[filterType]))
    if conditionQuery:
        baseQuery += ' where %s'%' AND '.join(conditionQuery)
       
    baseQuery += ' order by starttime desc'

    params['result'] = p.api.model.core.job.query(baseQuery)

def match(q, i, params, tags):
    return True

