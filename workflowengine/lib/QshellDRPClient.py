"""
from pymonkey import q

from osis import init
from osis.model.serializers import ThriftSerializer
from osis.client.xmlrpc import XMLRPCTransport
from osis.client import OsisConnection
from osis.store.OsisDB import OsisDB
from osis.store.OsisFilterObject import OsisFilterObject

class DRPClient(object):
    '''
    The DRPClient is the only point of access to the OSIS database. All objects of DRPClient share the same state.
    '''
    __shared_state = {}
    
    def __init__(self):
        self.__dict__ = self.__shared_state
        if not hasattr(self, 'initialized'):
            self.__initialize()
            setattr(self, 'initialized', True)
    
    def __initialize(self):
        init(q.system.fs.joinPaths(q.dirs.baseDir, 'libexec','osis'))
        
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
"""
