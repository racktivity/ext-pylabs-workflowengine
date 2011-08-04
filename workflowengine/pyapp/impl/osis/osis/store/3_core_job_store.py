__author__ = 'Incubaid'

def main(q, i, p, params, tags):
    osis = p.application.getOsisConnection(p.api.appname) 
    viewname = '%s_view_%s_list' % (params['domain'], params['rootobjecttype'])
    rootobject = params['rootobject']
    values = dict()
    values['name'] = rootobject.name
    values['parentjobguid'] = rootobject.parentjobguid
    values['joborder'] = rootobject.order
    values['description'] = rootobject.description
    values['actionname'] = rootobject.actionName
    values['usererrormsg'] = rootobject.userErrormsg
    values['internalerrormsg'] = rootobject.internalErrormsg
    values['maxduration'] = rootobject.maxduration
    values['jobstatus'] = rootobject.jobstatus
    values['starttime'] = rootobject.starttime
    values['endtime'] = rootobject.endtime
    values['clouduserguid'] = rootobject.clouduserguid
    values['rootobjecttype'] = rootobject.rootobjecttype
    values['rootobjectguid'] = rootobject.rootobjectguid
    values['agentguid'] = rootobject.agentguid
    values['log'] = rootobject.log
    values['creationdate'] = rootobject.creationdate
    
    osis.viewSave(params['domain'], params['rootobjecttype'], viewname, rootobject.guid, rootobject.version, values)

def match(q, i, params, tags):
    return params['rootobjecttype'] == 'job' and params['domain'] == 'core'
