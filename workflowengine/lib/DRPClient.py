from pymonkey import q

from osis import init
from osis.model.serializers import ThriftSerializer
from osis.client.xmlrpc import XMLRPCTransport
from osis.client import OsisConnection
from osis.store.OsisFilterObject import OsisFilterObject

from concurrence import Tasklet, Message

class MSG_DRP_CALL(Message): pass
class MSG_DRP_RETURN(Message): pass
class MSG_DRP_EXCEPTION(Message): pass
        
class DRPClient():
    ''' A dummy DRPClient, the connectDRPClient of DRPTask can be used to initialize the client. '''

class DRPInterface():
    ''' The connectDRPClient function of DRPTask places this interface on the DRPClient for each root object. '''
    
    def __init__(self, tasklet, name):
        self.__drpTasklet = tasklet
        self.__name = name
        
    def get(self, guid, version=None):
        return self.__sendToDrpTasklet('get', guid, version)
    
    def save(self, object_):
        return self.__sendToDrpTasklet('save', object_)
    
    def new(self, *args, **kwargs):
        return self.__sendToDrpTasklet('new', *args, **kwargs)
    
    def find(self, filter_, view=None):
        return self.__sendToDrpTasklet('find', filter_, view)
    
    def findAsView(self, filter_, viewName):
        return self.__sendToDrpTasklet('findAsView', filter_, viewName)

    def query(self, query):
        return self.__sendToDrpTasklet('query', query)

    @staticmethod
    def getFilterObject():
        return OsisFilterObject()
    
    def __sendToDrpTasklet(self, *args, **kwargs):
        MSG_DRP_CALL.send(self.__drpTasklet)(Tasklet.current(), self.__name, *args, **kwargs)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_DRP_RETURN):
            return args[0]
        elif msg.match(MSG_DRP_EXCEPTION):
            raise args[0]


class DRPTask:
    
    def __init__(self, address, service):
        try:
            init(q.system.fs.joinPaths(q.dirs.baseDir, 'libexec','osis'))
        except AssertionError:
            pass #OSIS already initialized
        
        try:
            self.connection = OsisConnection(XMLRPCTransport(address, service), ThriftSerializer)
        except:
            q.logger.log("[DRPTask] Failed to initialize the OSIS application server service connection.", 1)
            raise
        
        self.__tasklet = None
    
    def connectDRPClient(self, drpClient):
        '''
        Connect the DRPClient to this tasklet. As a result, the DRPClient will send his DRP messages to the tasklet in this task.
        @raise Exception: if the DRPTask is not yet started.
        '''
        if self.__tasklet == None:
            q.logger.log("[DRPTask] The DRPTask is not yet started, can't connect the drpClient.", 1)
            raise Exception("The DRPTask is not yet started, can't connect the drpClient.")
        
        from osis import ROOTOBJECT_TYPES as types
        for type in types.itervalues():
            name = getattr(type, 'OSIS_TYPE_NAME', type.__name__.lower())
            setattr(drpClient, name, DRPInterface(self.__tasklet, name))
    
    def start(self):
        self.__tasklet = Tasklet.new(self.__run)()
    
    def __run(self):
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_DRP_CALL):
                (caller, rootobject, action) = args[0:3]
                try:
                    #If you want to add transaction support, this is the place to be.
                    result = getattr(getattr(self.connection, rootobject), action)(*args[3:], **kwargs)
                except Exception, e:
                    q.logger.log("[DRPTasklet] Exception occured on DRP action: " + str(e), 1)
                    MSG_DRP_EXCEPTION.send(caller)(e)
                else:
                    MSG_DRP_RETURN.send(caller)(result)
