class AMQPAbstraction:
    '''
    This class provides an abstraction of an AMQP lib.
    Following functions are abstracted: queue_declare, exchange_declare and queue_bind.
    '''
    def __init__(self, queue_declare, exchange_declare, queue_bind):
        self.queue_declare = queue_declare
        self.exchange_declare = exchange_declare
        self.queue_bind = queue_bind

class QueueInfrastructure:
    '''
    The QueueInfrastructure creates the basic RabbitMQ exchanges and queues required by the workflowengine.
    This class is independent of the AMQP-lib.
    
    Every node has to call createBasicInfrastructure, to make sure that the basic infrastructure is in place,
    before waiting on queues or sending to exchanges. 
    
    The basic infrastructure contains
        For Appserver - WFE communication:
        * WFE_RPC_EXCHANGE: the exchange to send a message to a WFE.
        * WFE_RPC_QUEUE: the queue containing the messages for a WFE.
        * WFE_RPC_RETURN_EXCHANGE: the exchange on which the WFE sends return messages for rpc messages. 
        For WFE - OSIS communication:
        * ...
    
    '''
    WFE_RPC_EXCHANGE = "wfe_rpc_exchange"
    WFE_RPC_QUEUE = "wfe_rpc_queue"
    WFE_RPC_RETURN_EXCHANGE = "wfe_rpc_return_exchange"
    WFE_RPC_RETURN_QUEUE_PREFIX = "wfe_rpc_rq_"
    
    WFE_OSIS_EXCHANGE = "wfe_osis_exchange"
    WFE_OSIS_QUEUE = "wfe_osis_queue"
    WFE_OSIS_RETURN_EXCHANGE = "wfe_osis_return_exchange"
    WFE_OSIS_RETURN_QUEUE_PREFIX = "wfe_osis_rq_"
    
    def __init__(self, amqpAbstraction):
        self.amqpAbstraction = amqpAbstraction
    
    def createBasicInfrastructure(self):
        '''
        Creates the basic infrastructure. This function contains the calls to declare and bind queues and exchanges.
        This function can not do any error handling, as this is library dependant.
        '''
        # Create the basic Appserver - WFE infrastructure
        self.amqpAbstraction.queue_declare(queue=self.WFE_RPC_QUEUE, durable=False, exclusive=False, auto_delete=False)
        self.amqpAbstraction.exchange_declare(exchange=self.WFE_RPC_EXCHANGE, type="direct", durable=False, auto_delete=False)
        self.amqpAbstraction.queue_bind(queue=self.WFE_RPC_QUEUE, exchange=self.WFE_RPC_EXCHANGE)
        self.amqpAbstraction.exchange_declare(exchange=self.WFE_RPC_RETURN_EXCHANGE, type="direct", durable=False, auto_delete=False)
        # Create the basic WFE - OSIS infrastructure
        self.amqpAbstraction.queue_declare(queue=self.WFE_OSIS_QUEUE, durable=False, exclusive=False, auto_delete=False)
        self.amqpAbstraction.exchange_declare(exchange=self.WFE_OSIS_EXCHANGE, type="direct", durable=False, auto_delete=False)
        self.amqpAbstraction.queue_bind(queue=self.WFE_OSIS_QUEUE, exchange=self.WFE_OSIS_EXCHANGE)
        self.amqpAbstraction.exchange_declare(exchange=self.WFE_OSIS_RETURN_EXCHANGE, type="direct", durable=False, auto_delete=False)
    
    def createAppserverReturnQueue(self, guid):
        returnQueueName = self.getAppserverReturnQueueName(guid) 
        self.amqpAbstraction.queue_declare(queue=returnQueueName, durable=False, exclusive=True, auto_delete=True)
        self.amqpAbstraction.queue_bind(queue=returnQueueName, exchange=self.WFE_RPC_RETURN_EXCHANGE, routing_key=guid)
    
    def createOsisReturnQueue(self, guid):
        returnQueueName = self.getOsisReturnQueueName(guid) 
        self.amqpAbstraction.queue_declare(queue=returnQueueName, durable=False, exclusive=True, auto_delete=True)
        self.amqpAbstraction.queue_bind(queue=returnQueueName, exchange=self.WFE_OSIS_RETURN_EXCHANGE, routing_key=guid)
    
    @classmethod
    def getAppserverReturnQueueName(klass, guid):
        return klass.WFE_RPC_RETURN_QUEUE_PREFIX + guid
    
    @classmethod
    def getOsisReturnQueueName(klass, guid):
        return klass.WFE_OSIS_RETURN_QUEUE_PREFIX + guid
        
