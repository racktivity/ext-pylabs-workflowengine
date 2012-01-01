from pylabs import q

from concurrence import Tasklet, Message

from amqplib import client_0_8 as amqp

from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure, getAmqpConfig
from workflowengine.protocol import RpcMessage, decode_message


# Monkey patch amqp to use Concurrence Transport
from workflowengine.amqp import create_concurrence_transport
amqp.connection.create_transport = create_concurrence_transport
create_transport = create_concurrence_transport

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass
class MSG_RETURN_MESSAGE_RECEIVED(Message): pass

class AMQPTask:
    '''
    The AMQPTask starts two tasklets: one is listening for incomming AMQP messages, the other is responsible for sending messages.

    The AMQPTask can receive and send dicts: the dicts are serialized and deserialized using yaml.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''

    def __init__(self, receiveQueue, sendExchange, tag):
        
        self.config = getAmqpConfig()
        
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
                q.logger.log("[AMQPTask] Trying to connect to RabbitMQ", 3)
                               
                self.connection = amqp.Connection(host=self.config['amqp_host']+":"+str(self.config['amqp_port']), \
                    userid=self.config['amqp_login'], password=self.config['amqp_password'], virtual_host=self.config['amqp_vhost'], 
                    insist=False, useConcurrence=True)

                # Successfully connected
                self.channel = self.connection.channel()
                
                # Setup the queue infrastructure
                queueInfrastructure = QueueInfrastructure(AMQPAbstraction(self.channel.queue_declare, self.channel.exchange_declare, self.channel.queue_bind))
                
                queueInfrastructure.createBasicInfrastructure()
                
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
        if hasattr(self, 'channel') and self.channel:
            self.channel.basic_cancel(self.tag)
            self.channel.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()

    def __receive_callback(self, msg):
        try:
            data = decode_message(msg.body)
        except Exception, e:
            q.logger.log("[AMQPTask] Received bad formatted data: " + str(msg) +
                    " error: " + e.message, 3)
        else:
            Tasklet.new(self.__messageHandler)(data)

    def __receive(self):
        # Called in the receiving tasklet
        q.logger.log("[AMQPTask] Ready to receive", 3)
        self.channel.basic_consume(queue=self.receiveQueue, no_ack=True, callback=self.__receive_callback, consumer_tag=self.tag)
        while True:
            self.channel.wait() # Waits for 1 message

    def __send(self):
        # Started in the sending tasklet
        q.logger.log("[AMQPTask] Ready to send", 3)
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                data =args[0]
                routing_key = args[1]
                print 'SEND ', self.sendExchange, routing_key, data
                q.logger.log("[AMQPTask] Sending message: " + data, 3)
                self.channel.basic_publish(amqp.Message(data), exchange=self.sendExchange, routing_key=routing_key)
            elif msg.match(MSG_SOCKET_CLOSE):
                return
