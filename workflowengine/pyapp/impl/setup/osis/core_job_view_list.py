__author__ = 'Incubaid'
__tags__ = 'setup'
__priority__= 3

from osis.store.OsisDB import OsisDB

def main(q, i, params, tags):
    rootobject = 'job'
    domain = "core"
    appname = params['appname']
    connection = OsisDB().getConnection(appname)
    if not connection.viewExists(domain, rootobject, rootobject):
        view = connection.viewCreate(domain, rootobject, rootobject)
        view.setCol('parentjobguid', q.enumerators.OsisType.UUID, True, index=True)
        view.setCol('joborder', q.enumerators.OsisType.INTEGER, True, index=True)
        view.setCol('name', q.enumerators.OsisType.STRING, True)
        view.setCol('description', q.enumerators.OsisType.STRING, True)
        view.setCol('actionname', q.enumerators.OsisType.STRING, True, index=True)
        view.setCol('usererrormsg', q.enumerators.OsisType.STRING, True)
        view.setCol('internalerrormsg', q.enumerators.OsisType.STRING, True)
        view.setCol('maxduration', q.enumerators.OsisType.INTEGER, True)
        view.setCol('jobstatus', q.enumerators.OsisType.STRING, True)
        view.setCol('starttime', q.enumerators.OsisType.DATETIME, True, index=True)
        view.setCol('endtime', q.enumerators.OsisType.DATETIME, True, index=True)
        view.setCol('clouduserguid', q.enumerators.OsisType.UUID, True, index=True)
        view.setCol('rootobjecttype', q.enumerators.OsisType.STRING, True)
        view.setCol('rootobjectguid', q.enumerators.OsisType.UUID, True)
        view.setCol('agentguid', q.enumerators.OsisType.STRING, True, index=True)
        view.setCol('log',q.enumerators.OsisType.TEXT, True)
        view.setCol('creationdate',q.enumerators.OsisType.DATETIME, True, index=True)
        connection.viewAdd(view)
