from workflowengine import getAppName
from pylabs import p

def getAmqpConfig():
        
        config = {}
        
        # AMQP Broker config
        appname = getAppName()
        config['amqp_host'] = p.application.getRabbitMqHost(appname)
        config['amqp_port'] = 5672
        config['amqp_login'] = 'guest'
        config['amqp_password'] = 'guest'
        config['amqp_vhost'] = '/'
        config['amqp_key'] = 'wfe1'
        config['appname'] = appname
        
        return config

class AMQPAbstraction:
    '''
    This class provides an abstraction of an AMQP lib.
    Following functions are abstracted: queue_declare, exchange_declare and queue_bind.
    '''
    def __init__(self, queue_declare, exchange_declare, queue_bind):
        self.queue_declare = queue_declare
        self.exchange_declare = exchange_declare
        self.queue_bind = queue_bind

class QueueInfrastructure(object):
    '''
    The QueueInfrastructure creates the basic RabbitMQ exchanges and queues required by the workflowengine.
    This class is independent of the AMQP-lib.

    Every node has to call createBasicInfrastructure, to make sure that the basic infrastructure is in place,
    before waiting on queues or sending to exchanges.

    The basic infrastructure contains
        For Appserver - WFE communication:
        * wfe_rpc_exchange: the exchange to send a message to a WFE.
        * wfe_rpc_queue: the queue containing the messages for a WFE.
        * wfe_rpc_return_exchange: the exchange on which the WFE sends return messages for rpc messages. 
        For WFE - OSIS communication:
        * ...

    '''    

    def __init__(self, amqpAbstraction=None):
        
        config = getAmqpConfig()
        
        self.amqpAbstraction = amqpAbstraction
        
        self.wfe_rpc_exchange = "%s.rpc" % config['appname']
        self.wfe_rpc_queue = "%s.rpc.%s" % (config['appname'], config['amqp_key'])
        self.wfe_rpc_return_exchange = "%s.rpc.return" % config['appname']
        self.wfe_rpc_return_queue_prefix = "%s.rpc.return." % config['appname']


    def createBasicInfrastructure(self):
        '''
        Creates the basic infrastructure. This function contains the calls to declare and bind queues and exchanges.
        This function can not do any error handling, as this is library dependant.
        '''
        # Create the basic Appserver - WFE infrastructure
        self.amqpAbstraction.exchange_declare(exchange=self.wfe_rpc_return_exchange, type="direct", durable=False, auto_delete=False)
        self.amqpAbstraction.exchange_declare(exchange=self.wfe_rpc_exchange, type="topic", durable=False, auto_delete=False)
        
        self.amqpAbstraction.queue_declare(queue=self.wfe_rpc_queue, durable=False, exclusive=False, auto_delete=False)
        self.amqpAbstraction.queue_bind(queue=self.wfe_rpc_queue, exchange=self.wfe_rpc_exchange, routing_key='%s.#' % self.wfe_rpc_queue)

    def createAppserverReturnQueue(self, guid):
        returnQueueName = self.getAppserverReturnQueueName(guid) 
        self.amqpAbstraction.queue_declare(queue=returnQueueName, durable=False, exclusive=False, auto_delete=True)
        self.amqpAbstraction.queue_bind(queue=returnQueueName, exchange=self.wfe_rpc_return_exchange, routing_key=returnQueueName)

    def getAppserverReturnQueueName(self, guid):
        return self.wfe_rpc_return_queue_prefix + guid


