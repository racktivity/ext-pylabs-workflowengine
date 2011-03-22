from pylabs import q

from concurrence import Tasklet, Message
from concurrence.io import BufferedStream, Socket, Server

import yaml

class MSG_SOCKET_SEND(Message): pass
class MSG_SOCKET_CLOSE(Message): pass

class SocketTaskHandler(object):
    '''
    The SocketTask starts a tasklet that listens for incomming connections. Only one connection will be granted access.
    When a client is connected, a second tasklet will be started. One tasklet is responsible for receiving messages, the other for sending messages.

    The SocketTask can receive and send dicts: the dicts are serialized and deserialized using yaml, messages are seperated using '\n---\n'.
    When the task receives a message, it will create a new tasklet and call the messageHandler, passing it the received data as a parameter.
    '''

    def __init__(self):
        #self.__server_socket = Socket.new()
        #self.__server_socket.set_reuse_address(1)
        #self.__server_socket.bind(('', port))
        #self.__server_socket.listen()
        
        self.__client_socket = None
        self.__receiving_tasklet = None
        self.__sending_tasklet = None

    def setMessageHandler(self, messageHandler):
        '''
        Set the callback that will be called when a message is received. The callback will be passed a dictionary.
        '''
        self.__messageHandler = messageHandler

    def start(self, client_socket):
        ''' Start the task: start one tasklet listing for incomming connections. '''
        
        #self.__receiving_tasklet = Tasklet.new(self.__serve)()
        
        self.__client_socket = client_socket
        
        self.__stream = BufferedStream(self.__client_socket)
        q.logger.log("[SocketTaskHandler] Client connected.", 3)
        self.__sending_tasklet = Tasklet.new(self.__send)()
        self.__receiving_tasklet = Tasklet.new(self.__receive())()

    def sendData(self, data):
        '''
        Send data over the socket.
        @param data: the data to send
        @raise Exception: if no client is connected
        '''
        
        if self.__sending_tasklet == None:
            q.logger.log("[SocketTaskHandler] No connection to client, can't send the message.", 1)
            raise Exception("No connection to client, can't send the message.")
        else:
            MSG_SOCKET_SEND.send(self.__sending_tasklet)(data)

    def stop(self):
        self.__stream and self.__stream.close()

    def __receive(self):
        # Called in the receiving tasklet
        reader = self.__stream.reader

        try:
            buffer = ""
            processData = True
            while True:
                line = reader.read_line()
                if line <> '---':
                    buffer += line + "\n"
                else:
                    try:
                        data = yaml.load(buffer)
                        processData = True

                        if data == 'ping':
                            processData = False
                            self.sendData('pong')

                    except yaml.parser.ParserError:
                        q.logger.log("[SocketTaskHandler] Received bad formatted data: " + str(buffer), 3)
                    else:
                        if processData:
                            q.logger.log("[SocketTaskHandler] Received data: " + str(data), 5)
                            Tasklet.new(self.__messageHandler)(data, self)
                    buffer = ""

        except EOFError:
            q.logger.log("[SocketTaskHandler] Client disconnected.", 3)
            MSG_SOCKET_CLOSE.send(self.__sending_tasklet)()
            self.__stream.close()

    def __send(self):
        # Started in the sending tasklet
        writer = self.__stream.writer

        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_SOCKET_SEND):
                message = self.__yaml_message(args[0])
                q.logger.log("[SocketTaskHandler] Sending message: " + message, 5)
                writer.write_bytes(message)
                writer.flush()
            elif msg.match(MSG_SOCKET_CLOSE):
                return

    def __yaml_message(self, dict):
        return yaml.dump(dict) + "\n---\n"
    
class SocketTask(object):
    
    def __init__(self, port, handler=None):
        q.logger.log("[SocketTask] Initializing on port %s" % port, 8)
        self.port = port
        self.handler = handler
        
        # Keep mapping between re
        self.connected_clients = dict()
        q.logger.log("[SocketTask] Initialized on port %s" % self.port, 8)
        
    def start(self):
        self.socket_server = Server.serve(('', self.port), self._handle_client_connection)
        q.logger.log("[SocketTask] Started on port %s" % self.port, 8)
    
    def stop(self):
        self.socket_server.close()
        q.logger.log("[SocketTask] Stopped on port %s" % self.port, 8)
    
    def setMessageHandler(self, handler):
        self.handler = handler
        
        
    def _handle_client_connection(self, client_socket):
        q.logger.log("[SocketTask] Connection request on %s" % self.port, 8)
        tasklet_handler = SocketTaskHandler()
        tasklet_handler.setMessageHandler(self.handler)
        tasklet_handler.start(client_socket)
        q.logger.log("[SocketTask] Connection handled on %s" % self.port, 8)
        
