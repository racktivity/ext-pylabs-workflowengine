__author__ = 'Incubaid'
__tags__ = 'setup'
__priority__= 3

from osis.store.OsisDB import OsisDB
from osis.store import OsisConnection

def main(q, i, params, tags):
    rootobject = 'job'
    domain = "core"
    appname = params['appname']
    scheme_name = OsisConnection.getSchemeName(domain = domain, objType = rootobject)
    view_name = OsisConnection.getTableName(domain = domain, objType = rootobject)
    connection = OsisDB().getConnection(appname)
    if not connection.viewExists(domain, rootobject, view_name):
        view = connection.viewCreate(domain, rootobject, view_name)
        view.setCol('parentjobguid', q.enumerators.OsisType.UUID, True)
        view.setCol('joborder', q.enumerators.OsisType.INTEGER,True)
        view.setCol('name', q.enumerators.OsisType.STRING, True)
        view.setCol('description', q.enumerators.OsisType.STRING, True)
        view.setCol('actionname', q.enumerators.OsisType.STRING, True)
        view.setCol('usererrormsg', q.enumerators.OsisType.STRING, True)
        view.setCol('internalerrormsg', q.enumerators.OsisType.STRING, True)
        view.setCol('maxduration', q.enumerators.OsisType.INTEGER, True)
        view.setCol('jobstatus', q.enumerators.OsisType.STRING, True)
        view.setCol('starttime', q.enumerators.OsisType.DATETIME, True)
        view.setCol('endtime', q.enumerators.OsisType.DATETIME, True)
        view.setCol('clouduserguid', q.enumerators.OsisType.UUID, True)
        view.setCol('rootobjecttype', q.enumerators.OsisType.STRING, True)
        view.setCol('rootobjectguid', q.enumerators.OsisType.UUID, True)
        view.setCol('agentguid', q.enumerators.OsisType.STRING, True)
        view.setCol('log',q.enumerators.OsisType.TEXT,True)
        view.setCol('creationdate',q.enumerators.OsisType.DATETIME,True)
        connection.viewAdd(view)

        indexes = ['actionname', 'agentguid','starttime', 'endtime', 'clouduserguid','parentjobguid','creationdate','joborder']

        for field in indexes:
            context = {'schema': scheme_name, 'view': view_name, 'field': field}
            connection.runQuery("CREATE INDEX %(view)s_%(field)s ON %(schema)s.%(view)s (%(field)s)" % context)
