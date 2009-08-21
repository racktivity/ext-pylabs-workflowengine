from pymonkey import q

from osis import init
from osis.model.serializers import ThriftSerializer
from osis.client.xmlrpc import XMLRPCTransport
from osis.client import OsisConnection
from osis.store.OsisDB import OsisDB
from osis.store.OsisFilterObject import OsisFilterObject

class DRPClient():
    '''
    The DRPClient is the only point of access to the OSIS database. All objects of DRPClient share the same state.
    '''
    __shared_state = {}
    
    def __init__(self):
        self.__dict__ = self.__shared_state
        if not hasattr(self, 'inited'):
            self.__initialize()
            setattr(self, 'inited', True)
    
    def __initialize(self):
        try:
            init(q.system.fs.joinPaths(q.dirs.baseDir, 'libexec','osis'))
        except AssertionError:
            pass #OSIS already initialized
        
        try:
            self.__connection = OsisConnection(XMLRPCTransport('http://localhost:8888', 'osis_service'), ThriftSerializer)
        except:
            q.logger.log("[DRPClient] Failed to initialize the OSIS application server service connection: the DRPClient won't work...", 1)
            return
        
        try:
            self.__conn = OsisDB().getConnection('main')
        except:
             q.logger.log("[DRPClient] Failed to initialize the database connection for OSIS: the DRPClient won't work...", 1)
             return

        from osis import ROOTOBJECT_TYPES as types
        for type in types.itervalues():
            name = getattr(type, 'OSIS_TYPE_NAME', type.__name__.lower())
            setattr(self, name, getattr(self.__connection, name))

        setattr(self, 'job', JobInterface(getattr(self, 'job'), self.__conn))
    

class JobInterface():
    
    def __init__(self, osisclient, conn):
        self.__osisclient = osisclient
        self.__conn = conn
        self.__createParentViewIfNonExisting()
        
    def __createParentViewIfNonExisting(self):
        if not self.__conn.viewExists('job', 'job_parentview'):
            job_parentview = self.__conn.viewCreate('job', 'job_parentview')
            job_parentview.setCol('jobguid', q.enumerators.OsisType.UUID, False)
            job_parentview.setCol('parentjobguid', q.enumerators.OsisType.UUID, False)
            self.__conn.viewAdd(job_parentview)
    
    def get(self, guid, version=None):
        return self.__osisclient.get(guid, version)
    
    def save(self, object_):
        self.__osisclient.save(object_)
        if object_.parentjobguid:
            self.__conn.viewSave('job','job_parentview', object_.guid, object_.version, {'jobguid':object_.guid, 'parentjobguid':object_.parentjobguid})
    
    def new(self, *args, **kwargs):
        return self.__osisclient.new(*args, **kwargs)
    
    @staticmethod
    def getFilterObject():
        return OsisFilterObject()
    
    def find(self, filter_, view=None):
        return self.__osisclient.find(filter_, view)
    
    def findChildren(self, parentjobguid):
        filterObj = self.getFilterObject()
        filterObj.add('job_parentview', 'parentjobguid', parentjobguid)
        
        childrenguids = self.find(filterObj)
        if childrenguids == [[],[]]:
            childrenguids = []
        childrenguids = set(childrenguids)
        
        return map(lambda x: self.get(x), childrenguids)

    def printJobTree(self, parentjobguid, indent=0):
        job = self.get(parentjobguid)
        print " "*indent + job.guid + " " + job.actionName + " " + str(job.jobstatus) + " " + job.log
        children = self.findChildren(parentjobguid)
        for child in children:
            self.printJobTree(child.guid, indent+1)
            

    def getNextChildOrder(self, parentjobguid):
        highestOrder = -1
        for child in self.findChildren(parentjobguid):
            if child.order > highestOrder:
                highestOrder = child.order
        return highestOrder + 1

