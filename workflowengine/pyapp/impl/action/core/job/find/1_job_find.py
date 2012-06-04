__author__ = 'incubaid'
__priority__= 3

from osis.store import OsisConnection

def main(q, i, p, params, tags):
    # rootobjecttype: machine, application, datacenter
    # fromTime/endTime: YYYY-MM-DD hh:mm:ss
    table = OsisConnection.getTable(domain = 'core', objType = 'job')
    baseQuery = 'select guid as jobguid, clouduserguid, rootobjectguid, description, parentjobguid, viewguid,\
                 jobstatus, rootobjecttype, actionname, agentguid, starttime, endtime, "name" from %s' % table
    conditionQuery = list()
    filters = ['name', 'actionname', 'description', 'jobstatus', 'agentguid', 'clouduserguid',
                                 'rootobjectguid', 'rootobjecttype', 'fromTime', 'toTime', 'parentjobguid']
    filterOptions = {'fromTime' :   ['starttime','>= '],
                     'toTime'   :   ['endtime','<= ']}
    for filterType in filters:
        value = params.get(filterType, '')
        if value not in ('',0):
            default = [filterType, "="]
            filter_ = filterOptions.get(filterType, default)
            conditionQuery.append("%s %s '%s'" % (filter_[0], filter_[1], value))
    if conditionQuery:
        baseQuery += ' where %s'%' AND '.join(conditionQuery)
       
    baseQuery += ' order by starttime desc'

    params['result'] = p.api.model.core.job.query(baseQuery)

def match(q, i, params, tags):
    return True

