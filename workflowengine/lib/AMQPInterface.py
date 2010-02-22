from pymonkey import q

from concurrence import Tasklet, Message

from amqplib import client_0_8 as amqp

import yaml
import traceback

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass

class AMQPTask:
    '''
    The AMQPTask starts two tasklets: one is listening for incomming AMQP messages, the other is responsible for sending messages. 
    
    The AMQPTask can receive and send dicts: the dicts are serialized and deserialized using yaml.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''
    
    def __init__(self, host, port, username, password, vhost):
        # TODO Figure out the exception handling for AMQPlib, what happens if RabbitMQ is not available etc.
        self.connection = amqp.Connection(host=host+":"+str(port), userid=username, password=password, virtual_host=vhost, insist=False, useConcurrence=True)
        self.channel = self.connection.channel()
        self.__setupAMQP()
        self.__receiving_tasklet = None
        self.__sending_tasklet = None
    
    def __setupAMQP(self):
        q.logger.log("[AMQPTask] Setting up AMQP queues.", 3)
        print "[AMQPTask] Setting up AMQP queues."
        
        # First we setup the input queue and exchange
        self.channel.queue_declare(queue="in_q", durable=False, exclusive=False, auto_delete=False)
        self.channel.exchange_declare(exchange="in_x", type="direct", durable=False, auto_delete=False)
        self.channel.queue_bind(queue="in_q", exchange="in_x")

        # Then we setup the output queue and exchange
        self.channel.queue_declare(queue="out_q", durable=False, exclusive=False, auto_delete=False)
        self.channel.exchange_declare(exchange="out_x", type="direct", durable=False, auto_delete=False)
        self.channel.queue_bind(queue="out_q", exchange="out_x")
    
    def setMessageHandler(self, messageHandler):
        '''
        Set the callback that will be called when a message is received. The callback will be passed a dictionary.
        '''
        self.__messageHandler = messageHandler
    
    def start(self):
        ''' Start the task: start the two tasklets. '''
        self.__receiving_tasklet = Tasklet.new(self.__receive)()
        self.__sending_tasklet = Tasklet.new(self.__send)()
    
    def sendData(self, data):
        '''
        Send data to the queue.
        @param data: the data to send
        @raise Exception: if no client is connected
        '''
        if self.__sending_tasklet == None:
            q.logger.log("[AMQPTask] No connection to client, can't send the message.", 1)
            print "[AMQPTask] No connection to client, can't send the message."
            raise Exception("No connection to client, can't send the message.")
        else:
            MSG_SOCKET_SEND.send(self.__sending_tasklet)(data)
    
    def stop(self):
         self.channel.basic_cancel("nodetag")
         self.channel.close()
         self.connection.close()
    
    def __recveive_callback(self, msg):
        msg = msg.body
        try:
            data = yaml.load(msg)
        except yaml.parser.ParserError:
            q.logger.log("[AMQPTask] Received bad formatted data: " + str(msg), 3)
            print "[AMQPTask] Received bad formatted data: " + str(msg)
        else:
            q.logger.log("[AMQPTask] Received data: " + str(data), 3)
            print "[AMQPTask] Received data: " + str(data)
            Tasklet.new(self.__messageHandler)(data)
    
    def __receive(self):
        # Called in the receiving tasklet
        # TODO Add some logics for reconnecting if the connection is lost !
        q.logger.log("[AMQPTask] READY TO RECEIVE !", 3)
        print "[AMQPTask] READY TO RECEIVE !"
        self.channel.basic_consume(queue='in_q', no_ack=True, callback=self.__recveive_callback, consumer_tag="nodetag")
        while True:
            try:
                self.channel.wait()
            except:
                print traceback.format_exc()
                MSG_SOCKET_CLOSE.send(self.__sending_tasklet)()
                q.logger.log("[AMQPTask] Client disconnected.", 3)
                print "[AMQPTask] Client disconnected."
                break
    
    def __send(self):
        # Started in the sending tasklet
        q.logger.log("[AMQPTask] READY TO SEND !", 3)
        print "[AMQPTask] READY TO SEND !"
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                message = yaml.dump(args[0])
                q.logger.log("[AMQPTask] Sending message: " + message, 3)
                print "[AMQPTask] Sending message: " + message
                self.channel.basic_publish(amqp.Message(message), exchange="out_x")
            elif msg.match(MSG_SOCKET_CLOSE):
                return

