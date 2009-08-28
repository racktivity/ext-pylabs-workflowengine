import yaml, threading
from twisted.internet import protocol, reactor

from pymonkey import q, i

class WFLActionManager():
    """
    This implementation of the ActionManager is available to the cloudAPI: only root object actions are available.
    """
    def __init__(self):
        self.factory = YamlClientFactory(self._receivedData)
        config = i.config.stacklesssocket.getConfig('main')
        
        def _do_connect():
            reactor.connectTCP(config['host'], int(config['port']), self.factory)
        reactor.callInThread(_do_connect)
        
        self.running = {}
        
        self.lock = threading.Lock()
        self.id = 0

    def _receivedData(self, data):
        self.running[data['id']]['return'] = data['return']
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
        self.lock.acquire()
        my_id = self.id
        self.id += 1
        self.lock.release()
        
        my_lock = threading.Lock()
        self.running[my_id] = {'lock': my_lock}
        my_lock.acquire()
        self.factory.sendData({'id':my_id, 'rootobjectname':rootobjectname, 'actionname':actionname, 'params':params, 'executionparams':executionparams, 'jobguid':jobguid})
        my_lock.acquire()
        # Wait for receivedData to release the lock
        my_lock.release()
        
        return self.running.pop(my_id)['return']


class ActionUnavailableException(Exception):
    def __init__(self):
        Exception.__init__(self, "This action is not available.")


class YamlProtocol(protocol.Protocol):
    lock = threading.Lock()
    
    def connectionMade(self):
        self.factory.instance = self

    def dataReceived(self, data):
        print "Received something"
        if self.factory.receivedCallback:
            messages = data.split("\n---\n")
            for message in messages:
                if message:
                    print message
                    retdata = yaml.load(message)
                    self.factory.receivedCallback(retdata)
    
    def connectionLost(self, reason):
        self.factory.instance = None
        
    def sendData(self, data):
        message = yaml.dump(data)
        message += "\n---\n"
        self.lock.acquire()
        self.transport.write(message)
        self.lock.release()

class YamlClientFactory(protocol.ReconnectingClientFactory):
    protocol = YamlProtocol
    instance = None
    
    maxDelay = 30
    
    def __init__(self, receivedCallback):
        self.receivedCallback = receivedCallback
    
    def sendData(self, data):
        if self.instance == None:
            raise Exception('Not connected to the stackless WFL.')
        else:
            self.instance.sendData(data)
