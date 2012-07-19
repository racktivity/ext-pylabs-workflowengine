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
        view.setCol('parentjobguid', q.enumerators.OsisType.UUID, False, index=True)
        view.setCol('joborder', q.enumerators.OsisType.INTEGER, False, index=True)
        connection.viewAdd(view)
