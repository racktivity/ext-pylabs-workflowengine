import yaml

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, defer
from twisted.internet.protocol import ClientCreator
from twisted.internet.error import ConnectionRefusedError

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate, Closed
from txamqp.content import Content
import txamqp.spec

from workflowengine.Exceptions import ActionNotFoundException
from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure

from workflowengine import getAppName

from pylabs import q

ActionManagerTaskletPath = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','tasklets')
ActorActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'actor')
RootobjectActionTaskletPath = q.system.fs.joinPaths(ActionManagerTaskletPath, 'rootobject')

appserver_return_guid = "abc" # TODO Should be a generated GUID
APPSERVER_CONSUMER_TAG = "app_tag"

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

    def _receivedData(self, msg):
        if msg.messageid not in self.deferreds:
            q.logger.log("[CLOUDAPIActionManager] Got message for an unknown id !")
        else:
            try:
                # @tod: check error or not!
                q.logger.log("[CLOUDAPIActionManager] Got message for id  !" )
                self.deferreds[msg.messageid].callback(msg.params['result'])
            except Exception, ex:
                q.logger.log('MISERIE: %s' % ex.message)
                raise ex
            #if 'return' in msg:
            #    self.deferreds[msg.messageid].callback(msg.result)
            #elif 'exception' in data:
            #    self.deferreds[msg.messageid].errback(msg.result)

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
        my_id = str(self.id)
        self.id += 1


        message = q.messagehandler.getRPCMessageObject()
        
        message.domain = 'cloudapi'
        message.category = rootobjectname
        message.methodname = actionname
        message.params = params
        message.params['executionparams'] = executionparams
        message.params['jobguid'] = jobguid
        
        message.login = ''
        message.passwd = ''
        
        # @todo: switch to guids
        message.messageid = my_id
        
        

        deferred = defer.Deferred()
        self.deferreds[my_id] = deferred 
        #self.amqpClient.sendMessage({'id':my_id, 'rootobjectname':rootobjectname, 'actionname':actionname, 'params':params, 'executionparams':executionparams, 'jobguid':jobguid})
        
        self.amqpClient.sendMessage(message)
        
        return deferred

    def startRootobjectActionSynchronous(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):

        return self.startRootobjectAction(rootobjectname, actionname, params, executionparams, jobguid)
    
        """
        q.logger.log('>>> Executing startRootobjectActionSynchronous : %s %s %s' % (rootobjectname, actionname, params), 1)
    	if not self.__engineLoaded:
    	    raise Exception(self.__error)
    
        if len(self.__taskletEngine.find(tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)) == 0:
            raise ActionNotFoundException("RootobjectAction", rootobjectname, actionname)

        self.__taskletEngine.execute(params, tags=(rootobjectname, actionname), path=RootobjectActionTaskletPath)

        result = {'jobguid': None, 'result': params.get('result', None)}

        q.logger.log('>>> startRootobjectActionSynchronous returns : %s ' % result, 1)

        return result
        """

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
        try:
            self.connection = yield ClientCreator(reactor, AMQClient, delegate=TwistedDelegate(), vhost=self.vhost, spec=self.spec).connectTCP(self.host, self.port)
            yield self.connection.authenticate(self.username, self.password)
        except ConnectionRefusedError:
            q.logger.log("[CLOUDAPIActionManager] Problem while connecting with RabbitMQ... Trying again in 10 seconds.")
            reactor.callLater(10, self.connect)
        except Closed:
            q.logger.log("[CLOUDAPIActionManager] Problem while connecting with RabbitMQ... Trying again in 10 seconds.")
            reactor.callLater(10, self.connect)
        else:
            q.logger.log("[CLOUDAPIActionManager] txAMQP authenticated.")
            self.channel = yield self.connection.channel(1)
            yield self.channel.channel_open()
            
            # The make_serial function was created in order to make a generic QueueInfrastructure.
            deferreds = []
            def make_serial(function):
                # Input: function that returns deferred
                # Output: function that doesn't call input function directly but serializes it:
                #         the first function that is executed will launch if deferreds[0].callback is called,
                #         the second function will launch if the first function is done, and so on
                # Note: the user has to append a final callback to deferreds, this will be called if the last function is done executing
                def new_func(*args, **kwargs):
                    index = len(deferreds)
                    deferreds.append(defer.Deferred())
                    def to_be_called(dummy):
                        function(*args, **kwargs).addCallback(lambda x: deferreds[index+1].callback(None))
                    deferreds[index].addCallback(to_be_called)
                return new_func
            
            amqpAbstraction = AMQPAbstraction(make_serial(self.channel.queue_declare), make_serial(self.channel.exchange_declare), make_serial(self.channel.queue_bind))
            queueInfrastructure = QueueInfrastructure(amqpAbstraction)
            queueInfrastructure.createBasicInfrastructure()
            queueInfrastructure.createAppserverReturnQueue(appserver_return_guid)
            self.returnQueueName = queueInfrastructure.getAppserverReturnQueueName(appserver_return_guid)
            
            deferreds.append(defer.Deferred())
            deferreds[-1].addCallback(self.__initialized)
            # All deferreds are created and will be serialized: set everything in motion !
            deferreds[0].callback(None)
    
    @inlineCallbacks
    def __initialized(self, ret):
        yield self.channel.basic_consume(queue=self.returnQueueName, no_ack=True, consumer_tag=APPSERVER_CONSUMER_TAG)
        self.queue = yield self.connection.queue(APPSERVER_CONSUMER_TAG)
        self.queue.get().addCallbacks(self.__gotMessage, self.__lostConnection)
        q.logger.log("[CLOUDAPIActionManager] txAMQP initialized. Ready to receive messages.")
        
    def __gotMessage(self, msg):
        try:
            #retdata = yaml.load(msg.content.body) # TODO Implement your favorite messaging format here
            message = q.messagehandler.getRPCMessageObject(msg.content.body)
            self.dataReceivedCallback(message)
            
        except Exception, ex:
            
            q.logger.log("[CLOUDAPIActionManager] txAMQP received invalid message: %s\nMsg: %s" % (ex, str(msg)), 3)
            q.logger.log("[CLOUDAPIActionManager] Exception:: " + ex.message, 3)
            
        finally:
            self.queue.get().addCallbacks(self.__gotMessage, self.__lostConnection)

    def __lostConnection(self, exception):
        q.logger.log("[CLOUDAPIActionManager] Connection with RabbitMQ was lost... Trying again in 10 seconds.")
        self.connection = None
        reactor.callLater(10, self.connect)

    def sendMessage(self, msg):
        if not hasattr(self, "connection") or self.connection is None:
            raise Exception("txAMQP has no connection...")

        #data['return_guid'] = appserver_return_guid 
        #message = Content(yaml.dump(data)) # TODO Implement your favorite messaging format here
        
        # @todo: define correct return queue!
        msg.returnqueue = appserver_return_guid
        message = Content(msg.getMessageString())
        
        routingkey = '%s.%s.%s' % (QueueInfrastructure.WFE_RPC_EXCHANGE, msg.category, msg.methodname)
        
        q.logger.log("[CLOUDAPIActionManager] txAMQP is sending the message " + str(message))
        ret = self.channel.basic_publish(exchange=QueueInfrastructure.WFE_RPC_EXCHANGE, routing_key=routingkey, content=message)
        
        
