from pymonkey import q

from concurrence import Tasklet, Message

from amqplib import client_0_8 as amqp
from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure

import yaml, base64

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass
class MSG_RETURN_MESSAGE_RECEIVED(Message): pass
 
class AMQPTask:
    '''
    The AMQPTask starts two tasklets: one is listening for incomming AMQP messages, the other is responsible for sending messages. 
    
    The AMQPTask can receive and send dicts: the dicts are serialized and deserialized using yaml.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''
    
    def __init__(self, host, port, username, password, vhost, receiveQueue, sendExchange, tag):
        (self.host, self.port, self.username, self.password, self.vhost) = (host, port, username, password, vhost)
        (self.receiveQueue, self.sendExchange, self.tag) = (receiveQueue, sendExchange, tag)
        self.__receiving_tasklet = None
        self.__sending_tasklet = None
        self.__osisReturnQueuesToCreate = [] # Contains the guids of the osis return queues that have to be created.
    
    def createOsisReturnQueue(self, osis_return_queue_guid):
        ''' Create an return queue for OSIS messages, this function has to be called before starting the task. '''
        self.__osisReturnQueuesToCreate.append(osis_return_queue_guid)
    
    def setMessageHandler(self, messageHandler):
        '''
        Set the callback that will be called when a message is received. The callback will be passed a dictionary.
        '''
        self.__messageHandler = messageHandler
    
    def start(self):
        ''' Start the task: start the two tasklets. '''
        self.__receiving_tasklet = Tasklet.new(self.__run)()
        
    def __run(self):
        while True:
            try:
                q.logger.log("[AMQPTask] Tyring to connect to RabbitMQ", 3)
                self.connection = amqp.Connection(host=self.host+":"+str(self.port), userid=self.username, password=self.password, virtual_host=self.vhost, insist=False, useConcurrence=True)
                # Successfully connected
                self.channel = self.connection.channel()
                # Setup the queue infrastructure
                queueInfrastructure = QueueInfrastructure(AMQPAbstraction(self.channel.queue_declare, self.channel.exchange_declare, self.channel.queue_bind))
                queueInfrastructure.createBasicInfrastructure()
                for osis_return_queue_guid in self.__osisReturnQueuesToCreate:
                    queueInfrastructure.createOsisReturnQueue(osis_return_queue_guid)
                
                self.__sending_tasklet = Tasklet.new(self.__send)()
                self.__receive()
            except IOError:
                #This error is raised if the connection was refused or abruptly closed
                q.logger.log("[AMQPTask] Connection with RabbitMQ was refused or abruptly close... Sleeping 10 seconds before reconnect.", 3)
                if self.__sending_tasklet is not None:
                    MSG_SOCKET_CLOSE.send(self.__sending_tasklet)()
                    self.__sending_tasklet = None
                Tasklet.sleep(10)
            except amqp.exceptions.AMQPException:
                #This exception is raised if the connection was closed correctly
                q.logger.log("[AMQPTask] Connection with RabbitMQ was closed correctly... Sleeping 10 seconds before reconnect.", 3)
                if self.__sending_tasklet is not None:
                    MSG_SOCKET_CLOSE.send(self.__sending_tasklet)()
                    self.__sending_tasklet = None
                Tasklet.sleep(10)
    
    def sendData(self, data, routing_key=''):
        '''
        Send data to the queue.
        @param data: the data to send
        @param routing_key: the routing key to use for the exchange
        @raise Exception: if no client is connected
        '''
        if self.__sending_tasklet == None:
            q.logger.log("[AMQPTask] No connection to AMQP, can't send the message.", 1)
            raise Exception("No connection to client, can't send the message.")
        else:
            MSG_SOCKET_SEND.send(self.__sending_tasklet)(data, routing_key)
    
    def stop(self):
         self.channel.basic_cancel(self.tag)
         self.channel.close()
         self.connection.close()
    
    def __recveive_callback(self, msg):
        msg = msg.body
        try:
            #data = yaml.load(msg)
            data = q.messagehandler.getRPCMessageObject(msg)
        #except yaml.parser.ParserError:
        except Exception, e:
            q.logger.log("[AMQPTask] Received bad formatted data: " + str(msg), 3)
        else:
            #q.logger.log("[AMQPTask] Received data: " + str(data), 3)
            Tasklet.new(self.__messageHandler)(data)
    
    def __receive(self):
        # Called in the receiving tasklet
        q.logger.log("[AMQPTask] Ready to receive", 3)
        self.channel.basic_consume(queue=self.receiveQueue, no_ack=True, callback=self.__recveive_callback, consumer_tag=self.tag)
        while True:
            self.channel.wait() # Waits for 1 message
    
    def __send(self):
        # Started in the sending tasklet
        q.logger.log("[AMQPTask] Ready to send", 3)
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                data =args[0]
                routing_key = args[1]
                q.logger.log("[AMQPTask] Sending message: " + data, 3)
                self.channel.basic_publish(amqp.Message(data), exchange=self.sendExchange, routing_key=routing_key)
            elif msg.match(MSG_SOCKET_CLOSE):
                return
    

class AMQPTransport(object):
    '''AMQP transport to communicate with an OSIS worker'''
    def __init__(self, host, port, username, password, vhost, return_queue_guid):
        ''' Initialize a new AMQP transport '''
        self.amqpTask = AMQPTask(host, port, username, password, vhost, QueueInfrastructure.getOsisReturnQueueName(return_queue_guid), QueueInfrastructure.WFE_OSIS_EXCHANGE, "osis_tag")
        self.amqpTask.createOsisReturnQueue(return_queue_guid)
        self.amqpTask.setMessageHandler(self.receive)
        self.amqpTask.start()
        
        # Hack Hack
        self.proxies = {} 
        
        self.return_queue_guid = return_queue_guid
        self.id = 0
        self.tasklets = {}
    
    def receive(self, message):
        MSG_RETURN_MESSAGE_RECEIVED.send(self.tasklets[message.messageid])(message)
    
    def sendAndWait(self, message):
        messageId = str(self.id)
        self.id += 1
               
        self.tasklets[messageId] = Tasklet.current()
        
        msg = q.messagehandler.getRPCMessageObject()
        msg.domain = 'drp'
        msg.category = message.get('type', '')
        msg.methodname = message.pop('action')
        
        msg.returnqueue = self.return_queue_guid
        msg.messageid = messageId
        
        msg.login = ''
        msg.passwd = ''
        
        for k, v in message.iteritems():
            if k not in ('type'):
                msg.params[k] = v
    
        #routingkey = '%s.%s.%s.%s' % (self.rpc_exchange, msg.domain, msg.category, msg.methodname)
        routingkey = '%s.%s.%s' % ('drp.rpc', msg.category, msg.methodname)
    
        
        self.amqpTask.sendData(msg.getMessageString(), routingkey)
        
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_RETURN_MESSAGE_RECEIVED):
                return args[0]
                #return args[0]['return']