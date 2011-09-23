from pylabs import q, p

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

    def __init__(self, domain, name, drp):
        self._domain = domain
        self.__name = name
        self.__drp = drp

    def get(self, guid):
        return self.__drp.sendToDrp(self._domain, self.__name, 'get', guid)

    def save(self, object_):
        return self.__drp.sendToDrp(self._domain, self.__name, 'save', object_)

    def new(self, *args, **kwargs):
        return self.__drp.sendToDrp(self._domain, self.__name, 'new', *args, **kwargs)

    def find(self, filter_, view=None):
        return self.__drp.sendToDrp(self._domain, self.__name, 'find', filter_, view)

    def findAsView(self, filter_, viewName):
        return self.__drp.sendToDrp(self._domain, self.__name, 'findAsView', filter_, viewName)

    def query(self, query):
        return self.__drp.sendToDrp(self._domain, self.__name, 'query', query)

    def delete(self, guid):
        return self.__drp.sendToDrp(self._domain, self.__name, 'delete', guid)

    @staticmethod
    def getFilterObject():
        return OsisFilterObject()

class BufferedDRPInterface():
    ''' The buffered drp interface postpones the stores of the objects. '''

    def __init__(self, domain, name, drp, buffer):
        self._domain = domain
        self.__name = name
        self.__drp = drp
        self.__buffer = buffer

    def get(self, guid):
        ''' Check buffer, not in buffer: go to osis '''
        if guid in self.__buffer:
            return self.__buffer[guid]
        else:
            return self.__drp.sendToDrp(self._domain, self.__name, 'get', guid)

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
        return self.__drp.sendToDrp(self._domain, self.__name, 'new', *args, **kwargs)

    def find(self, filter_, view=None):
        ''' Can't use find on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self._domain, self.__name, self.__buffer)
        return self.__drp.sendToDrp(self._domain, self.__name, 'find', filter_, view)

    def findAsView(self, filter_, viewName):
        ''' Can't use findAsView on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self._domain, self.__name, self.__buffer)
        return self.__drp.sendToDrp(self._domain, self.__name, 'findAsView', filter_, viewName)

    def query(self, query):
        ''' Can't use query on the buffer: first commit everything to osis, then find '''
        self.__drp.commit_buffer(self._domain, self.__name, self.__buffer)
        return self.__drp.sendToDrp(self._domain, self.__name, 'query', query)

    def delete(self, guid):
        ''' Delete the object from the queue and from osis '''
        self.__buffer.pop(guid)
        return self.__drp.sendToDrp(self._domain, self.__name, 'delete', guid)

    @staticmethod
    def getFilterObject():
        return OsisFilterObject()


class DRPTask:

    bufferedObjects = [ 'core.job' ]

    def __init__(self, address, service):
        try:
            #self.connection = p.api.model.transport
            #OsisConnection(XMLRPCTransport(address, service), ThriftSerializer)
            self.connection = dict()
        except:
            q.logger.log("[DRPTask] Failed to initialize the OSIS application server service connection.", 1)
            raise

        self.__tasklet = None
        
        self.__buffers = {}
        self.__buffer_tasklet = None

    def connectDRPClient(self, drpClient, rootobjects=None):
        '''
        Connect the DRPClient to this tasklet. As a result, the DRPClient will send his DRP messages to the tasklet in this task.
        @raise Exception: if the DRPTask is not yet started.
        '''
        if self.__tasklet == None:
            q.logger.log("[DRPTask] The DRPTask is not yet started, can't connect the drpClient.", 1)
            raise Exception("The DRPTask is not yet started, can't connect the drpClient.")

        from pymodel import ROOTOBJECT_TYPES as types
        for domain, domain_types in types.iteritems():
            for name, type_ in domain_types.iteritems():
                if rootobjects and (domain, name) not in rootobjects:
                    continue
                #name = getattr(type_, 'OSIS_TYPE_NAME', type_.__name__.lower())
                name_ = '%s.%s' % (domain, name)
                self.connection[(domain, name)] = getattr(getattr(drpClient,
                    domain), name)
                if name_ in self.bufferedObjects:
                    buffer = {}
                    self.__buffers[(domain, name)] = buffer
                    setattr(getattr(drpClient, domain), name,
                        BufferedDRPInterface(domain, name, self, buffer))
                else:
                    setattr(getattr(drpClient, domain), name,
                        DRPInterface(domain, name, self))

    def start(self):
        self.__tasklet = Tasklet.new(self.__run)()
        if len(self.bufferedObjects) > 0:
            self.__buffer_tasklet = Tasklet.new(self.__buffer_run)()

    def sendToDrp(self, domain, name, *args, **kwargs):
        MSG_DRP_CALL.send(self.__tasklet)(Tasklet.current(), domain, name,
            *args, **kwargs)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_DRP_RETURN):
            return args[0]
        elif msg.match(MSG_DRP_EXCEPTION):
            raise args[0]

    def __run(self):
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_DRP_CALL):
                (caller, domain, rootobject, action) = args[0:4]
                q.logger.log("[DRPTasklet] Received task: ro=" + str(rootobject) + " action=" + str(action), 5)
                try:
                    #If you want to add transaction support, this is the place to be.

                    args_ = args[4:]
                    kwargs_ = kwargs
                    orig_model_obj = self.connection[(domain, rootobject)]
                    callable_ = getattr(orig_model_obj, action)

                    #print 'Calling', callable_, 'using', args_, 'and', kwargs_

                    result = callable_(*args_, **kwargs_)

                    #print 'Result is', result
                except Exception, e:
                    q.logger.log("[DRPTasklet] Exception occured on DRP action: " + str(e), 1)
                    MSG_DRP_EXCEPTION.send(caller)(WFLException.create(e))
                else:
                    MSG_DRP_RETURN.send(caller)(result)
    
    def __buffer_run(self):
        ''' Flush the buffers to OSIS every second '''
        while True:
            for (domain, buffer_name) in self.__buffers:
                self.commit_buffer(domain, buffer_name,
                    self.__buffers[(domain, buffer_name)])
            Tasklet.sleep(1)
    
    def commit_buffer(self, domain, name, buffer):
        ''' Commit 1 buffer to OSIS '''
                
        for k in buffer.keys():
            
            # ------------------------Start unsafe ---------------------------
            object_ = buffer.pop(k, None)
            
            # Check if we have an object
            # Object could  be saved in the mean time by other calls to commit_buffer
            # Triggered by timed calls or direct calls in the buffered drp interface
            if object_:
                try:
                    self.sendToDrp(domain, name, 'save', object_)
                except Exception, ex:
                    q.logger.log('[DRPTasklet] Failed to save buffered %s with guid %s' % (name, k))
                    
                    # Re-add to buffer if it does not already contain a (newer?) version
                    # As versions are guids, we can't tell if a version is newer or older
                    if not k in buffer:
                        buffer[k] = object_
            # ------------------------Stop unsafe ---------------------------
                    
                
                
            
            
        
