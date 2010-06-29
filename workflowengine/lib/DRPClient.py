from pymonkey import q

from osis import init
from osis.model.serializers import ThriftSerializer
from osis.client import OsisConnection
from osis.store.OsisFilterObject import OsisFilterObject

from concurrence import Tasklet, Message

from workflowengine.Exceptions import WFLException
from workflowengine.AMQPInterface import AMQPTransport

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
        q.logger.log('>>>>>>> Saving DRP object %s (sending msg to tasklet)' % object_)
        return self.__drp.sendToDrp(self.__name, 'save', object_)

    def new(self, *args, **kwargs):
        # @todo: fixme: temp workaround
        return getattr(q.pymodel.drp, self.__name).getEmptyModelObject()
        #return self.__drp.sendToDrp(self.__name, 'new', *args, **kwargs)

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
        # @todo: fixme: temp workaround
        return getattr(q.pymodel.drp, self.__name).getEmptyModelObject()
        #return self.__drp.sendToDrp(self.__name, 'new', *args, **kwargs)

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

###################################
# Duplicate Code
###################################
class WFEPymodelOsisClient(object):
    
    def __init__(self, model, client):
    
        self._model = model
        self._model_type = self._model._ROOTOBJECTTYPE.OSIS_MODEL_INFO.name
        self._client = client
    
    def get(self, guid):
        '''Retrieve a root object with a given GUID from the OSIS server

        If no version is specified, the latest version is retrieved.

        @param guid: GUID of the root object to retrieve
        @type guid: string

        @return: Root object instance
        @rtype: L{osis.model.RootObjectModel}
        '''
        
        params = {
                  'rootobjectguid': guid,
                  'rootobjecttype': self._model_type,
        }
        
        result = self._do_rpc_call('get', params)
        
        return self._model.thriftBase64Str2object(result['rootobject'])
        
    def query(self, query):
        
        ''' 
        run query from OSIS server

        @param query: Query to execute on OSIS server
        @type query: string

        @return: result of the query else raise error
        @type: List of rows. Each row shall be represented as a dictionary.
        '''
        
        params = {
                  'query': query,
        }
        
        result = self._do_rpc_call('query', params)
        
        return result['result']
        

    def delete(self, guid):
        '''Delete a root object with a given GUID from the OSIS server

        If no version is specified, all the versions shall be deleted.

        @param guid: GUID of the root object to delete
        @type guid: string
        @param version: Version GUID of the object to delete
        @type version: string

        @return: True or False, according as the deletion succeeds or fails
        '''
        
        params = {
                  'rootobjectguid': guid,
                  'rootobjecttype': self._model_type,
                  'rootobjectversionguid': None,        # @todo: remove: for compatibility with existing tasklets

        }
        
        result = self._do_rpc_call('delete', params)
        
        return result['result']

    def save(self, rootobject):
        '''Save a root object to the server

        @param object_: Object to store
        @type object_: L{osis.model.RootObjectModel}
        '''
        
        # @todo: tmp
        if not rootobject.guid:
            rootobject.guid = q.base.idgenerator.generateGUID() 
            
        params = {
                  'rootobject': self._model.object2ThriftBase64Str(rootobject),
                  'rootobjectguid': rootobject.guid,
                  'rootobjecttype': self._model_type,
        }
        
        result = self._do_rpc_call('store', params)
        
        return rootobject

    def find(self, filter_, view=None):
        '''Perform a find/filter operation

        If no view name is specified, a list of GUIDs of the matching root
        objects is returned. Otherwise a L{ViewResultList} is returned.

        @param filter_: Filter description
        @type filter_: OsisFilterObject
        @param view: View to return
        @type view: string

        @return: List of GUIDs or view result
        @rtype: tuple<string> or L{ViewResultList}
        '''
        
        params = {
                  'filterobject': filter_.filters,
                  'osisview': view,
                  'rootobjecttype': self._model_type,
        }
        
        result = self._do_rpc_call('findobject', params)
        
        return result['result']

    def findAsView(self, filter_, viewName):
        """
        Perform a find/filter operation.
        @param filter_: Filter description
        @type filter_: OsisFilterObject
        @param view: name of the view to return
        @type view: string

        @return: list of dicts representing the view{col: value}
        """
        
        params = {
                  'filterobject': filter_.filters,
                  'osisview': viewName,
                  'rootobjecttype': self._model_type,
        }
        
        result = self._do_rpc_call('findasview', params)
                
        return result['result']

    
    def new(self, *args, **kwargs): #pylint: disable-msg=W0142
        '''Create a new instance of the root object type

        All arguments are handled verbatim to the root object type constructor.
        '''
        return self._model._ROOTOBJECTTYPE(*args, **kwargs)

    @staticmethod
    def getFilterObject(): #pylint: disable-msg=C0103
        '''Create a new filter object instance'''
        return OsisFilterObject()
    
    
    def _do_rpc_call(self, methodname, params):
        
        # @todo: Use dispatcher to encode / decode rpc msg for async use
        return_msg = self._client.do_rpc_call_sync(self._model_type, methodname, params)
        
        if 'rpc_exception' in return_msg.params.keys():
            raise Exception(return_msg.params['rpc_exception'])
            
        return return_msg.params
    
###################################
# Test WFE Client
class WFEOsisRpcMessageClient(object):
    
    def __init__(self, domain, transport, rpc_exchange=None, rpc_return_exchange=None):
        self.domain = domain 
        self.transport = transport
        self.rpc_exchange = rpc_exchange or '%s.rpc' % domain
        self.rpc_return_exchange = rpc_return_exchange or '%s.rpc.return' % domain
        self.sessionid = q.base.idgenerator.generateGUID()
        self.returnqueue = '%s.%s.%s' % (self.rpc_return_exchange, q.application.agentid, self.sessionid)
        
        
    def connect(self):
        self._client = q.queue.getRabbitMQConnection()
        
    def isconnected(self):
        return hasattr(self, '_client') and self._client
    
    # Hack Hack
    def do_rpc_call_sync(self, category, methodname, params):
        
        #if not self.isconnected():
        #    self.connect()
        
        
        params['type'] = category
        params['action'] = methodname
        
        msg =  self.transport.sendAndWait(params)
        
        return msg

class DRPTask:

    bufferedObjects = [] #[ 'job' ]

    def __init__(self, host, port, username, password, vhost):
        init(q.system.fs.joinPaths(q.dirs.baseDir, 'libexec','osis'))
        return_queue_guid = "abc" # Should be a generated GUID for scalability

        try:
            #self.connection = OsisConnection(AMQPTransport(host, port, username, password, vhost, return_queue_guid), ThriftSerializer)
            self.connection = AMQPTransport(host, port, username, password, vhost, return_queue_guid)
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

        # Hack Hack       
        self._DOMAIN = 'drp'
        
        #self._client = OsisRpcMessageClient(self._DOMAIN, 'osis.rpc', 'osis.rpc.return')
        self._client = WFEOsisRpcMessageClient(self._DOMAIN, self.connection)
        
        domain = getattr(q.pymodel, self._DOMAIN)
        
        for model in (getattr(domain, model) for model in dir(domain) if not model.startswith('_')):
            setattr(drpClient, model._ROOTOBJECTTYPE.OSIS_MODEL_INFO.name, WFEPymodelOsisClient(model, self._client))

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
                    #result = getattr(getattr(self.connection, rootobject), action)(*args[3:], **kwargs)
                    
                    
                    #result = getattr(self.connection, action)(*args[3:], **kwargs)
                    # Hack Hack
                    result = getattr(self.connection.proxies[rootobject], action)(*args[3:], **kwargs)
                    
                    
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
        

