import yaml, threading, time
from twisted.internet import protocol, reactor

from pymonkey import q, i

class WFLActionManager():
    """
    This implementation of the ActionManager is available to the cloudAPI: only root object actions are available.
    """
    def __init__(self):
        self.factory = YamlClientFactory(self._receivedData)
        config = i.config.workflowengine.getConfig('main')
        
        def _do_connect():
            reactor.connectTCP('localhost', int(config['port']), self.factory)
        reactor.callInThread(_do_connect)
        
        self.running = {}
        
        self.idlock = threading.Lock()
        self.id = 0

    def _receivedData(self, data):
        self.running[data['id']]['return'] = data.get('return')
        self.running[data['id']]['error'] = data.get('error')
        self.running[data['id']]['exception'] = data.get('exception')
        self.running[data['id']]['lock'].release()

    def startActorAction(self, actorname, actionname, params, executionparams={}, jobguid=None):
        '''
        This action is unavailable.
        @raise ActionUnavailableException: always thrown
        '''
        raise ActionUnavailableException()
        
    def startRootobjectAction(self, rootobjectname, actionname, params, executionparams={}, jobguid=None):
        """
        Send the root object action to the stackless workflowengine over a socket.
        The root object action will wait until the workflowengine returns a result.
        """
        self.idlock.acquire()
        my_id = self.id
        self.id += 1
        self.idlock.release()
        
        my_lock = threading.Lock()
        self.running[my_id] = {'lock': my_lock}
        my_lock.acquire()
        self.factory.sendData({'id':my_id, 'rootobjectname':rootobjectname, 'actionname':actionname, 'params':params, 'executionparams':executionparams, 'jobguid':jobguid})
        my_lock.acquire()
        # Wait for receivedData to release the lock
        my_lock.release()
        
        data = self.running.pop(my_id)
        if not data['error']:
            return data['return']
        else:
            raise data['exception']


class ActionUnavailableException(Exception):
    def __init__(self):
        Exception.__init__(self, "This action is not available.")


class YamlProtocol(protocol.Protocol):
    writelock = threading.Lock()
    delimiter = "\n---\n"
    buffer = ""
    
    
    def connectionMade(self):
        self.factory.instance = self

    def dataReceived(self, data):
        if self.factory.receivedCallback:
            self.buffer = self.buffer + data
            while self.delimiter in self.buffer:
                delimiter_index = self.buffer.find(self.delimiter)
                message = self.buffer[:delimiter_index]
                self.buffer = self.buffer[delimiter_index+len(self.delimiter):]
                if message:
                    try:
                        retdata = yaml.load(message)
                    except yaml.parser.ParserError:
                        q.logger.log("[CLOUDAPIActionManager] Socket received invalid message: " + str(message), 3)
                    else:
                        self.factory.receivedCallback(retdata)
    
    def connectionLost(self, reason):
        self.factory.instance = None
        
    def sendData(self, data):
        message = yaml.dump(data)
        message += "\n---\n"
        self.writelock.acquire()
        self.transport.write(message)
        self.writelock.release()

class YamlClientFactory(protocol.ReconnectingClientFactory):
    protocol = YamlProtocol
    instance = None
    
    maxDelay = 30 # If the connection is lost, the factory will try to reconnect: the max delay between reconnects.

    timeout = 5 # sendData waits a number of seconds before raising the not connected exception.
    sleepBetweenChecks = 0.5 # sendData will try to sleep a number of seconds between each check.
    maxAttempts = timeout/sleepBetweenChecks
    
    def __init__(self, receivedCallback):
        self.receivedCallback = receivedCallback
    
    def sendData(self, data):
        # If a request for sendData is received: reset the reconnect timeout and retry 
        if self.connector and self.connector.state == 'disconnected':
            self.stopTrying()
            self.resetDelay()
            self.retry()
        
        # Let the sendData sleep if there is no connection.
        attempt = 0
        while not self.instance:
            if attempt == self.maxAttempts:
                break
            else:
                time.sleep(self.sleepBetweenChecks)
                attempt += 1
        
        # Either the connection is made or the timeout is reached.
        if self.instance == None:
            raise Exception('Not connected to the stackless WFL.')
        else:
            self.instance.sendData(data)
