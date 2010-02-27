from pymonkey import q

from concurrence import Tasklet, Message

from amqplib import client_0_8 as amqp
from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure

import yaml, socket

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass
 
WFE_CONSUMER_TAG = "wfe_tag"

class AMQPTask:
    '''
    The AMQPTask starts two tasklets: one is listening for incomming AMQP messages, the other is responsible for sending messages. 
    
    The AMQPTask can receive and send dicts: the dicts are serialized and deserialized using yaml.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''
    
    def __init__(self, host, port, username, password, vhost):
        (self.host, self.port, self.username, self.password, self.vhost) = (host, port, username, password, vhost)
        self.__receiving_tasklet = None
        self.__sending_tasklet = None
        
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
                # Setup the basic infrastructure
                amqpAbstraction = AMQPAbstraction(self.channel.queue_declare, self.channel.exchange_declare, self.channel.queue_bind)
                QueueInfrastructure(amqpAbstraction).createBasicInfrastructure()
                
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
    
    def sendData(self, return_guid, data):
        '''
        Send data to the queue.
        @param data: the data to send
        @raise Exception: if no client is connected
        '''
        if self.__sending_tasklet == None:
            q.logger.log("[AMQPTask] No connection to AMQP, can't send the message.", 1)
            raise Exception("No connection to client, can't send the message.")
        else:
            MSG_SOCKET_SEND.send(self.__sending_tasklet)(return_guid, data)
    
    def stop(self):
         self.channel.basic_cancel(WFE_CONSUMER_TAG)
         self.channel.close()
         self.connection.close()
    
    def __recveive_callback(self, msg):
        msg = msg.body
        try:
            data = yaml.load(msg)
        except yaml.parser.ParserError:
            q.logger.log("[AMQPTask] Received bad formatted data: " + str(msg), 3)
        else:
            q.logger.log("[AMQPTask] Received data: " + str(data), 3)
            Tasklet.new(self.__messageHandler)(data)
    
    def __receive(self):
        # Called in the receiving tasklet
        q.logger.log("[AMQPTask] Ready to receive", 3)
        self.channel.basic_consume(queue=QueueInfrastructure.WFE_RPC_QUEUE, no_ack=True, callback=self.__recveive_callback, consumer_tag=WFE_CONSUMER_TAG)
        while True:
            self.channel.wait() # Waits for 1 message
    
    def __send(self):
        # Started in the sending tasklet
        q.logger.log("[AMQPTask] Ready to send", 3)
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                return_guid = args[0]
                data = yaml.dump(args[1])
                q.logger.log("[AMQPTask] Sending message: " + data, 3)
                self.channel.basic_publish(amqp.Message(data), exchange=QueueInfrastructure.WFE_RPC_RETURN_EXCHANGE, routing_key=return_guid)
            elif msg.match(MSG_SOCKET_CLOSE):
                return

