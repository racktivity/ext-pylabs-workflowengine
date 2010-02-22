import yaml, time

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, defer
from twisted.internet.protocol import ClientCreator

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp.spec

from workflowengine.Exceptions import ActionNotFoundException

from pymonkey import q, i

ActionManagerTaskletPath = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','tasklets')
ActorActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'actor')
RootobjectActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'rootobject')

class WFLActionManager():
    """
    This implementation of the ActionManager is available to the cloudAPI: only root object actions are available.
    """
    def __init__(self):
        #TODO Load the config file here
        host = "localhost"
        port = 5672
        vhost = "/"
        username = "guest"
        password = "guest"
        
        self.amqpClient = AMQPInterface(host, port, vhost, username, password)
        self.amqpClient.setDataReceivedCallback(self._receivedData)
        self.amqpClient.connect()
        
        self.id = 0
        self.deferreds = {}
        
        ###### For synchronous execution ##########
        from pymonkey.tasklets import TaskletsEngine
    	try:
    	    self.__taskletEngine = TaskletsEngine()
    	    ##create tasklets dir if it doesnt exist
    	    if not q.system.fs.exists(ActorActionTaskletPath):
    		q.system.fs.createDir(ActorActionTaskletPath)
    	    self.__taskletEngine.addFromPath(ActorActionTaskletPath)
    	    if not q.system.fs.exists(RootobjectActionTaskletPath):
    		q.system.fs.createDir(RootobjectActionTaskletPath)
    	    self.__taskletEngine.addFromPath(RootobjectActionTaskletPath)
    	    self.__engineLoaded = True
    	except Exception, ex:
    	    self.__engineLoaded = False
    	    self.__error = ex
        ###### /For synchronous execution ##########

    def _receivedData(self, data):
        if data['id'] not in self.deferreds:
            q.logger.log("[CLOUDAPIActionManager] Got message for an unknown id !")
        else:
            if 'return' in data:
                self.deferreds[data['id']].callback(data['return'])
            elif 'exception' in data:
                self.deferreds[data['id']].errback(data['exception'])

    def startActorAction(self, actorname, actionname, params, executionparams={}, jobguid=None):
        '''
        This action is unavailable.
        @raise ActionUnavailableException: always thrown
        '''
        raise ActionUnavailableException()

    def startRootobjectAction(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        # For backwards compatibility
        # If called not explicitely, wait for result
        if not 'wait' in executionparams:
            executionparams['wait'] = True

        return self.startRootobjectActionAsynchronous(rootobjectname, actionname, params, executionparams, jobguid)

    def startRootobjectActionAsynchronous(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        """
        Send the root object action to the stackless workflowengine over a socket.
        The root object action will wait until the workflowengine returns a result.
        """

        # For backwards compatibility
        # If called explicitely, don't wait for result
        if not 'wait' in executionparams:
            executionparams['wait'] = False

        # Don't need to lock here, all async actions are called from the reactor thread
        my_id = self.id
        self.id += 1

        deferred = defer.Deferred()
        self.deferreds[my_id] = deferred 
        self.amqpClient.sendMessage({'id':my_id, 'rootobjectname':rootobjectname, 'actionname':actionname, 'params':params, 'executionparams':executionparams, 'jobguid':jobguid})
        
        return deferred

    def startRootobjectActionSynchronous(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):

        q.logger.log('>>> Executing startRootobjectActionSynchronous : %s %s %s' % (rootobjectname, actionname, params), 1)
    	if not self.__engineLoaded:
    	    raise Exception(self.__error)
    
            if len(self.__taskletEngine.find(tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)) == 0:
                raise ActionNotFoundException("RootobjectAction", rootobjectname, actionname)
    
            self.__taskletEngine.execute(params, tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)
    
            result = {'jobguid': None, 'result': params.get('result', None)}
    
            q.logger.log('>>> startRootobjectActionSynchronous returns : %s ' % result, 1)
    
            return result

class ActionUnavailableException(Exception):
    def __init__(self):
        Exception.__init__(self, "This action is not available.")

class AMQPInterface():

    def __init__(self, host, port, vhost, username, password):
        (self.host, self.port, self.vhost, self.username, self.password) = (host, port, vhost, username, password)
        self.spec = txamqp.spec.load("/opt/qbase3/lib/python2.6/site-packages/txamqp/amqp0-8.xml") #TODO Path should be generated !
        self.initialized = False

    def setDataReceivedCallback(self, callback):
        self.dataReceivedCallback = callback

    @inlineCallbacks
    def connect(self):
        self.connection = yield ClientCreator(reactor, AMQClient, delegate=TwistedDelegate(), vhost=self.vhost, spec=self.spec).connectTCP(self.host, self.port)
        yield self.connection.authenticate(self.username, self.password)
        q.logger.log("[CLOUDAPIActionManager] txAMQP authenticated. Ready to receive messages")
        self.channel = yield self.connection.channel(1)
        yield self.channel.channel_open()
        yield self.channel.basic_consume(queue='out_q', no_ack=True, consumer_tag="returnqueue") # TODO Which queue to read from ?
        self.queue = yield self.connection.queue("returnqueue")
        self.queue.get().addCallback(self.__gotMessage)
    
    def __gotMessage(self, msg):
        try:
            retdata = yaml.load(msg.content.body) # TODO Implement your favorite messaging format here
            self.dataReceivedCallback(retdata)
        except yaml.parser.ParserError:
            q.logger.log("[CLOUDAPIActionManager] txAMQP received invalid message: " + str(message), 3)
        finally:
            self.queue.get().addCallback(self.__gotMessage)

    def sendMessage(self, data):
        #TODO We should check if the connection is still open, etc.
        if not hasattr(self, "connection"):
            raise Exception("txAMQP has no connection...")
        
        message = Content(yaml.dump(data)) # TODO Implement your favorite messaging format here
        message["delivery mode"] = 2
        q.logger.log("[CLOUDAPIActionManager] txAMQP is sending the message " + str(data))
        self.channel.basic_publish(exchange="in_x", content=message, routing_key="test") # TODO Which queue to write to ?
        
