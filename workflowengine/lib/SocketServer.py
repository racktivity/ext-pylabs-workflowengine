from pymonkey import q

from concurrence import Tasklet, Message
from concurrence.io import BufferedStream, Socket

import yaml

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass

class SocketTask:
    '''
    The SocketTask starts a tasklet that listens for incomming connections. Only one connection will be granted access. 
    When a client is connected, a second tasklet will be started. One tasklet is responsible for receiving messages, the other for sending messages.
    
    The SocketTask can receive and send dicts: the dicts are serialized and deserialized using yaml, messages are seperated using '\n---\n'.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''
    
    def __init__(self, port):
        self.__server_socket = Socket.new()
        self.__server_socket.set_reuse_address(1)
        self.__server_socket.bind(('', port))
        self.__server_socket.listen()
        self.__client_socket = None
        
        self.__receiving_tasklet = None
        self.__sending_tasklet = None
    
    def setMessageHandler(self, messageHandler):
        '''
        Set the callback that will be called when a message is received. The callback will be passed a dictionary.
        '''
        self.__messageHandler = messageHandler
    
    def start(self):
        ''' Start the task: start one tasklet listing for incomming connections. '''
        self.__receiving_tasklet = Tasklet.new(self.__serve)()
    
    def sendData(self, data):
        '''
        Send data over the socket.
        @param data: the data to send
        @raise Exception: if no client is connected
        '''
        if self.__sending_tasklet == None:
            q.logger.log("[SocketTask] No connection to client, can't send the message.", 1)
            raise Exception("No connection to client, can't send the message.")
        else:
            MSG_SOCKET_SEND.send(self.__sending_tasklet)(data)
    
    def stop(self):
        self.__client_socket and self.__client_socket.close()
        self.__server_socket and self.__server_socket.close()
    
    def __serve(self):
        # Started in the receiving tasklet
        while True:
            self.__client_socket = self.__server_socket.accept()
            self.__stream = BufferedStream(self.__client_socket)
            q.logger.log("[SocketTask] Client connected.", 3)
            self.__sending_tasklet = Tasklet.new(self.__send)()
            self.__receive()

    def __receive(self):
        # Called in the receiving tasklet
        reader = self.__stream.reader
        
        try:
            buffer = ""
            while True:
                line = reader.read_line()
                if line <> '---':
                    buffer += line + "\n"
                else:
                    data = yaml.load(buffer)
                    q.logger.log("[SocketTask] Received data: " + str(data), 5)
                    Tasklet.new(self.__messageHandler)(data)    
                    buffer = ""
                    
        except EOFError:
            q.logger.log("[SocketTask] Client disconnected.", 3)
            MSG_SOCKET_CLOSE.send(self.__sending_tasklet)()
            self.__stream.close()
    
    def __send(self):
        # Started in the sending tasklet
        writer = self.__stream.writer
        
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                message = yaml.dump(args[0]) + "\n---\n"
                q.logger.log("[SocketTask] Sending message: " + message, 5)
                writer.write_bytes(message)
                writer.flush()
            elif msg.match(MSG_SOCKET_CLOSE):
                return
    
