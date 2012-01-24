import json

from concurrence.io import Connector
from concurrence.io.buffered import BufferedStream

from amqplib.client_0_8.transport import _AbstractTransport  
from amqplib.client_0_8.transport import AMQP_PORT, AMQP_PROTOCOL_HEADER

class ConcurrenceTransport(_AbstractTransport):
    
    def __init__(self, host, connect_timeout):
        '''connect_timeout is currently omitted'''
        
        msg = 'socket.getaddrinfo() for %s returned an empty list' % host
        port = AMQP_PORT
        
        if ':' in host:
            host, port = host.rsplit(':', 1)
            port = int(port)
        else:
            raise ValueError('Invalid host specified, expected host:port')

        self.sock = None
    
        print 'Connecting to %s on port %s' % (host, port)
        self.sock = Connector.connect((host, port))
            
        if not self.sock:
            # Didn't connect, return the most recent error message
            raise socket.error, msg

        self._setup_transport()

        self._write(AMQP_PROTOCOL_HEADER)
            
    def _read(self, n):
        """
        Read exactly n bytes from the peer
        """
        return self.stream.reader.read_bytes(n)

    def _setup_transport(self):
        """
        Do any additional initialization of the class (used
        by the subclasses).
        """
        self.stream = BufferedStream(self.sock, buffer_size=1024*1024)

    def _shutdown_transport(self):
        """
        Do any preliminary work in shutting down the connection.
        """
        self.stream.close()
        
        if not self.sock.is_closed():
            self.sock.close()

    def _write(self, s):
        """
        Completely write a string to the peer.

        """
        r = self.stream.writer.write_bytes(s)
        self.stream.writer.flush()
        return r
    
def create_concurrence_transport(host, connect_timeout, ssl=False):
    return ConcurrenceTransport(host, connect_timeout)
            