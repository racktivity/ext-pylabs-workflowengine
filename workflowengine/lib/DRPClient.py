from pymonkey import q

from osis import init
from osis.model.serializers import ThriftSerializer
from osis.client.xmlrpc import XMLRPCTransport
from osis.client import OsisConnection
from osis.store.OsisFilterObject import OsisFilterObject

from concurrence import Tasklet, Message

from workflowengine.Exceptions import WFLException

import uuid

class MSG_DRP_CALL(Message): pass
class MSG_DRP_RETURN(Message): pass
class MSG_DRP_EXCEPTION(Message): pass

class DRPClient():
    '''
    A dummy DRPClient, the connectDRPClient of DRPTask can be used to initialize the client.
    No interface is provided here: the root objects are read from the OSIS connection, which is not available at this time.
    '''

class DRPInterface():
    ''' The connectDRPClient function of DRPTask places this interface on the DRPClient for each root object. '''

    def __init__(self, name, drp):
        self.__name = name
        self.__drp = drp

    def get(self, guid, version=None):
        return self.__drp.sendToDrp(self.__name, 'get', guid, version)

    def save(self, object_):
        return self.__drp.sendToDrp(self.__name, 'save', object_)

    def new(self, *args, **kwargs):
        return self.__drp.sendToDrp(self.__name, 'new', *args, **kwargs)

    def find(self, filter_, view=None):
        return self.__drp.sendToDrp(self.__name, 'find', filter_, view)

    def findAsView(self, filter_, viewName):
        return self.__drp.sendToDrp(self.__name, 'findAsView', filter_, viewName)

    def query(self, query):
        return self.__drp.sendToDrp(self.__name, 'query', query)

    def delete(self, guid, version=None):
        return self.__drp.sendToDrp(self.__name, 'delete', guid, version)

    @staticmethod
    def getFilterObject():
        return OsisFilterObject()

class BufferedDRPInterface():
    ''' The buffered drp interface postpones the stores of the objects. '''

    def __init__(self, name, drp, buffer):
        self.__name = name
        self.__drp = drp
        self.__buffer = buffer

    def get(self, guid):
        ''' Check buffer, not in buffer: go to osis '''
        if guid in self.__buffer:
            return self.__buffer[guid]
        else:
            return self.__drp.sendToDrp(self.__name, 'get', guid)

    def save(self, object_):
        ''' Save the object in the buffer and give it a guid '''
        try:
            guid = object_.guid
        except AttributeError:
            guid = None
        if not guid:
            object_.guid = str(uuid.uuid4())
        
        self.__buffer[object_.guid] = object_

    def new(self, *args, **kwargs):
        ''' Create a new object, no need to use osis '''
        return self.__drp.sendToDrp(self.__name, 'new', *args, **kwargs)

    def find(self, filter_, view=None):
        ''' Can't use find on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self.__name, self.__buffer)
        return self.__drp.sendToDrp(self.__name, 'find', filter_, view)

    def findAsView(self, filter_, viewName):
        ''' Can't use findAsView on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self.__name, self.__buffer)
        return self.__drp.sendToDrp(self.__name, 'findAsView', filter_, viewName)

    def query(self, query):
        ''' Can't use query on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self.__name, self.__buffer)
        return self.__drp.sendToDrp(self.__name, 'query', query)

    def delete(self, guid):
        ''' Delete the object from the queue and from osis '''
        self.__buffer.pop(guid)
        return self.__drp.sendToDrp(self.__name, 'delete', guid)

    @staticmethod
    def getFilterObject():
        return OsisFilterObject()


class DRPTask:

    bufferedObjects = [ 'job' ]

    def __init__(self, address, service):
        init(q.system.fs.joinPaths(q.dirs.baseDir, 'libexec','osis'))

        try:
            self.connection = OsisConnection(XMLRPCTransport(address, service), ThriftSerializer)
        except:
            q.logger.log("[DRPTask] Failed to initialize the OSIS application server service connection.", 1)
            raise

        self.__tasklet = None
        
        self.__buffers = {}
        self.__buffer_tasklet = None

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
            if name in self.bufferedObjects:
                buffer = {}
                self.__buffers[name] = buffer
                setattr(drpClient, name, BufferedDRPInterface(name, self, buffer))
            else:
                setattr(drpClient, name, DRPInterface(name, self))

    def start(self):
        self.__tasklet = Tasklet.new(self.__run)()
        if len(self.bufferedObjects) > 0:
            self.__buffer_tasklet = Tasklet.new(self.__buffer_run)()

    def sendToDrp(self, name, *args, **kwargs):
        MSG_DRP_CALL.send(self.__tasklet)(Tasklet.current(), name, *args, **kwargs)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_DRP_RETURN):
            return args[0]
        elif msg.match(MSG_DRP_EXCEPTION):
            raise args[0]

    def __run(self):
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_DRP_CALL):
                (caller, rootobject, action) = args[0:3]
                q.logger.log("[DRPTasklet] Received task: ro=" + str(rootobject) + " action=" + str(action), 5)
                try:
                    #If you want to add transaction support, this is the place to be.
                    result = getattr(getattr(self.connection, rootobject), action)(*args[3:], **kwargs)
                except Exception, e:
                    q.logger.log("[DRPTasklet] Exception occured on DRP action: " + str(e), 1)
                    MSG_DRP_EXCEPTION.send(caller)(WFLException.create(e))
                else:
                    MSG_DRP_RETURN.send(caller)(result)
    
    def __buffer_run(self):
        ''' Flush the buffers to OSIS every second '''
        while True:
            for buffer_name in self.__buffers:
                self.commit_buffer(buffer_name, self.__buffers[buffer_name])
            Tasklet.sleep(1)
    
    def commit_buffer(self, name, buffer):
        ''' Commit 1 buffer to OSIS '''
        commit_buffer = {}
        commit_buffer.update(buffer)
        buffer.clear()
        
        for object_ in commit_buffer.values():
            self.sendToDrp(name, 'save', object_)
        
