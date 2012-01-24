import os.path

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor, defer
from twisted.internet.protocol import ClientCreator
from twisted.internet.error import ConnectionRefusedError

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate, Closed 
from txamqp.content import Content
import txamqp.spec

from workflowengine.Exceptions import ActionNotFoundException
from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure, getAmqpConfig
from workflowengine.protocol import RpcMessage, encode_message, decode_message
from workflowengine import getAppName

from pylabs import q, p

ActorActionTaskletPath = q.system.fs.joinPaths(q.dirs.baseDir, 'pyapps',
        getAppName(), 'impl', 'actor')
RootobjectActionTaskletPath = q.system.fs.joinPaths(q.dirs.baseDir, 'pyapps',
        getAppName(), 'impl', 'action')

class WFLActionManager():
    """
    This implementation of the ActionManager is available to the cloudAPI: only root object actions are available.
    """
    def getID(self):
        return "appserver1"
    
    def getRoutingKey(self, msg):
        """Determine correct routing key for action"""
        
        #@todo: move to tasklet
        routingkey = '%s.rpc.%s.%s.%s' % (self.config['appname'], "wfe1", msg.category, msg.methodname)
        
        return routingkey
    
    def __init__(self):
        
        self.appname = getAppName()
        
        # AMQP Broker config
        self.config = getAmqpConfig()
            
        self.amqpClient = AMQPInterface(self.config['amqp_host'], self.config['amqp_port'], \
            self.config['amqp_vhost'], self.config['amqp_login'], self.config['amqp_password'], self.getID())
        self.amqpClient.setDataReceivedCallback(self._receivedData)
        self.amqpClient.connect()
        
        self.id = 0
        self.deferreds = {}

        ###### For synchronous execution ##########
        try:
            ##create tasklets dir if it doesnt exist
            if not q.system.fs.exists(ActorActionTaskletPath):
                q.system.fs.createDir(ActorActionTaskletPath)
            if not q.system.fs.exists(RootobjectActionTaskletPath):
                q.system.fs.createDir(RootobjectActionTaskletPath)

            self.__taskletEngine = q.taskletengine.get(ActorActionTaskletPath)
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
                # @todo: check error or not!
                q.logger.log("[CLOUDAPIActionManager] Got message for id %s ! Result: %s" % (msg.messageid, msg.params['result']))
                d = self.deferreds.pop(msg.messageid) 
                d.callback(msg.params['result'])
            except Exception, ex:
                q.logger.log('[CLOUDAPIActionManager]: ERROR: %s' % ex.message)
                raise ex

    def startActorAction(self, domainname, actorname, actionname, params, executionparams={}, jobguid=None):
        '''
        This action is unavailable.
        @raise ActionUnavailableException: always thrown
        '''
        raise ActionUnavailableException()

    def startRootobjectAction(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):

        # For backwards compatibility
        # If called not explicitely, wait for result
        if not 'wait' in executionparams:
            executionparams['wait'] = True

        return self.startRootobjectActionAsynchronous(domainname, rootobjectname, actionname, params, executionparams, jobguid)

    def startRootobjectActionAsynchronous(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):
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
        self.id = (self.id + 1) % 32768

        message = RpcMessage()
        
        message.domain = domainname
        message.category = rootobjectname
        message.methodname = actionname
        message.params = params
        message.params['executionparams'] = executionparams
        message.params['jobguid'] = jobguid
        
        message.login = ''
        message.passwd = ''
        
        message.messageid = my_id

        deferred = defer.Deferred()
        self.deferreds[my_id] = deferred 
        
        self.amqpClient.sendMessage(message, self.getRoutingKey(message))
        
        return deferred

    def startRootobjectActionSynchronous(self, domainname, rootobjectname, actionname, params, executionparams={}, jobguid=None):        
        if not self.__engineLoaded:
            raise Exception(self.__error)

        path = os.path.join(RootobjectActionTaskletPath, domainname)
        tags = (domainname, rootobjectname, actionname)

        q.logger.log("Finding tasklets for path %s and tags %s" % (path, tags), 7)

        tasklets = self.__taskletEngine.find(tags=tags, path=path)
        if not tasklets:
            raise ActionNotFoundException("RootobjectAction", domainname, rootobjectname, actionname)

        self.__taskletEngine.execute(params, tags=tags, path=path)

        result = {'jobguid': None, 'result': params.get('result', None)}

        return result


class ActionUnavailableException(Exception):
    def __init__(self):
        Exception.__init__(self, "This action is not available.")

class AMQPInterface():

    def __init__(self, host, port, vhost, username, password, id):
        (self.host, self.port, self.vhost, self.username, self.password, self.id) = (host, port, vhost, username, password, id)
        self.spec = txamqp.spec.load(q.system.fs.joinPaths(q.dirs.cfgDir, 'amqp', 'amqp0-8.xml'))
        self.initialized = False
        
        self.config = getAmqpConfig()
        

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
                # Input: function that returns deferredfrom amqplib.client_0_8 import transport
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
            queueInfrastructure.createAppserverReturnQueue(self.id)
            
            self.returnQueueName = queueInfrastructure.getAppserverReturnQueueName(self.id)
            
            
            deferreds.append(defer.Deferred())
            deferreds[-1].addCallback(self.__initialized)
            # All deferreds are created and will be serialized: set everything in motion !
            deferreds[0].callback(None)
    
    @inlineCallbacks
    def __initialized(self, ret):
        yield self.channel.basic_consume(queue=self.returnQueueName, no_ack=True, consumer_tag=self.id)
        self.queue = yield self.connection.queue(self.id)
        self.queue.get().addCallbacks(self.__gotMessage, self.__lostConnection)
        q.logger.log("[CLOUDAPIActionManager] txAMQP initialized. Ready to receive messages.")
        
    def __gotMessage(self, msg):
        try:
            q.logger.log("[CLOUDAPIActionManager] gotMessage: %s" % msg)
            message = decode_message(msg.content.body)
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

    def sendMessage(self, msg, routingkey):
        if not hasattr(self, "connection") or self.connection is None:
            raise Exception("txAMQP has no connection...")

        # @todo: define correct return queue!
        msg.returnqueue = self.returnQueueName
        message = Content(encode_message(msg))
        
        q.logger.log("[CLOUDAPIActionManager] txAMQP is sending the message " + str(message))
        ret = self.channel.basic_publish(exchange="%s.rpc" % self.config['appname'], routing_key=routingkey, content=message)
        
        
