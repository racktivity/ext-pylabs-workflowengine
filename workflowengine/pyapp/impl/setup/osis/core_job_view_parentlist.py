__author__ = 'Incubaid'
__tags__ = 'setup'
__priority__= 3

from osis.store.OsisDB import OsisDB

def main(q, i, params, tags):
    rootobject = 'job'
    domain = "core"
    appname = params['appname']
    view_name = '%s_view_%s_parentlist' % (domain, rootobject)
    connection = OsisDB().getConnection(appname)
    if not connection.viewExists(domain, rootobject, view_name):
        view = connection.viewCreate(domain, rootobject, view_name)
        view.setCol('parentjobguid', q.enumerators.OsisType.UUID, False)
        view.setCol('joborder', q.enumerators.OsisType.INTEGER, False)
        connection.viewAdd(view)
        indexes = ['parentjobguid','joborder']

        for field in indexes:
            context = {'schema': "%s_%s" % (domain, rootobject), 'view': view_name, 'field': field}
            connection.runQuery("CREATE INDEX %(field)s_%(schema)s_%(view)s ON %(schema)s.%(view)s (%(field)s)" % context)
