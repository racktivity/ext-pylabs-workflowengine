from pymonkey import q

from twisted.words.protocols.jabber import xmlstream, client, jid
from twisted.words.xish import domish
from twisted.internet import reactor
from twisted.names.srvconnect import SRVConnector

class XMPPClient:
    '''
    The XMPPClients uses the username, server and password to connect to the xmpp server.
    
    A message can be send using the sendMessage function, received presences and messages
    can be processed by a callback function.
    
    If the connection fails or is lost, the client will try to reconnect automatically.
    The disconnectedCallback can be used to catch the connection failed or connection lost event.
    At startup, a watchdog is started: if the server can't authenticate the user within the
    timeOut, the connection will be closed and the disconnectedCallback will be called. 
    '''
    
    def __init__(self, username, server, password, timeOut=5):
        self.username = username
        self.server = server
        self.password = password
        self.timeOut = timeOut
        
        self.status = 'NOT_CONNECTED'
        
        self.messageReceivedCallback = None
        self.presenceReceivedCallback = None
        self.connectedCallback = None
        self.disconnectedCallback = None
        
        self.connector = None
        self.xmlstream = None
    
    def start(self):
        ''' Start the xmpp client, opens the connection to the server '''
        if self.status <> 'NOT_CONNECTED':
            raise RuntimeError('The XmppClient has already been started.')
        
        q.logger.log("[XMPPCLIENT] Starting the xmpp client to " + self.username + "@" + self.server, 5)
        
        self.startup_watchdog = reactor.callLater(self.timeOut, self._watchdog_timeout)
        self._connect()
    
    def stop(self):
        ''' Stop the xmpp client '''
        if self.status == 'NOT_CONNECTED':
            raise RuntimeError('The XmppClient has not yet been started.')
        
        q.logger.log("[XMPPCLIENT] Stopping the xmpp client to " + self.username + "@" + self.server, 5)
        self.connector.disconnect()
    
    def sendMessage(self, to, type, id, message=' '):
        ''' Send a message
        @param to: The username of the client to send the message to
        @type to: string
        @param type: The type of the message
        @type type: string
        @param id: The id of the message
        @type id: string
        @param message: The message to send
        @type message: string 
        '''
        if self.status <> 'RUNNING':
            raise NotConnectedException()
        
        q.logger.log("[XMPPCLIENT] Sending message '" + str(id) + "' of type '" + str(type) +"' to " + str(to) + " for " + self.username + "@" + self.server, 5)
        
        elemToSend = domish.Element(('jabber:client','message'), attribs={'to':to+"@"+self.server, 'type':type, 'id':id})
        body = domish.Element((None, 'body'))
        body.addContent(message)
        elemToSend.addContent(body)
        self.xmlstream.send(elemToSend)
    
    def sendPresence(self, to=None, type=None):
        ''' Send a presence
        @param to: The username of the client to send the presence to. None=send to all your friends
        @type to: string
        @param type: The type of the presence. Possible values: None=available, unavailable, subscribe, subscribed 
        @type type: string
        '''
        if self.status <> 'RUNNING':
            raise NotConnectedException()
        
        q.logger.log("[XMPPCLIENT] Sending presence of type '" + str(type) +"' to " + str(to) + "'", 5)
        
        attribs={}
        if to <> None: attribs['to'] =  to+"@"+self.server
        if type <> None: attribs['type'] = type
        
        presence = domish.Element(('jabber:client','presence'), attribs=attribs)
        self.xmlstream.send(presence)
    
    def _presence_received(self, elem):
        fromm = elem.getAttribute('from').split("@")[0]
        if not elem.hasAttribute('type'):
            type = 'available'
        else:
            type = elem.getAttribute('type')
            
        q.logger.log("[XMPPCLIENT] Presence received from '" + fromm + "' of type '" + type +"'", 5)
        
        if self.presenceReceivedCallback:
            self.presenceReceivedCallback(fromm, type)                
    
    def setMessageReceivedCallback(self, callback):
        '''
        @param callback: function to call when a message is received
        @type callback: function that expects 4 params: from, type, id and message
        '''
        self.messageReceivedCallback = callback
        
    def setPresenceReceivedCallback(self, callback):
        '''
        @param callback: function to call when a presence is received
        @type callback: function that expects 2 params: from and type
        '''
        self.presenceReceivedCallback = callback
    
    def setDisconnectedCallback(self, callback):
        '''
        Set the function to call when the client is disconnected from the xmpp server,
        if the keepAlive param is True in the constructor, the reconnect will be initiated after this callback.
        @param callback: function to call when the client is disconnected
        @type callback: function that expects 1 param: string containing the reason of the disconnect
        '''
        self.disconnectedCallback = callback
        
    def setConnectedCallback(self, callback):
        '''
        Set the function to call when the client is connected and authenticated with the xmpp server.
        @param callback: function to call when the client is connected and authenticated
        @type callback: function that expects no params
        '''
        self.connectedCallback = callback
    
    def _connect(self):
        self.status = 'CONNECTING'
        
        c = client.XMPPClientFactory(jid.JID(self.username+"@"+self.server), self.password)
        c.addBootstrap(xmlstream.STREAM_CONNECTED_EVENT, self._connected)
        c.addBootstrap(xmlstream.STREAM_AUTHD_EVENT, self._authenticated)
        c.addBootstrap(xmlstream.INIT_FAILED_EVENT, self._init_failed)
        c.addBootstrap(xmlstream.STREAM_END_EVENT, self._end_stream)

        def _do_connect():
            self.connector = SRVConnector(reactor,'xmpp-client' ,self.server, c)
            self.connector.connect()
    
        reactor.callInThread(_do_connect)
        
    def _connected(self, xmlstream):
        self.status = 'CONNECTED'
            
        q.logger.log("[XMPPCLIENT] Connected to server '" + self.server + "'", 5)
        
        # Connected: capture the stream
        self.xmlstream = xmlstream
        self.xmlstream.addObserver('/presence', self._presence_received)
        self.xmlstream.addObserver('/message',  self._message_received)
    
    def _authenticated(self, xmlstream):
        self.status = 'RUNNING'
        
        q.logger.log("[XMPPCLIENT] Server '" + self.server + "' authenticated user '" + self.username + "'", 5)
        
        # Authentication succes: stop the startup watchdog
        if self.startup_watchdog:
            self.startup_watchdog.cancel()
            self.startup_watchdog = None
        
        # Put the agent controllers status to online 
        self.sendPresence()
        
        if self.connectedCallback:
            self.connectedCallback()
    
    def _presence_received(self, elem):
        fromm = elem.getAttribute('from').split("@")[0]
        if not elem.hasAttribute('type'):
            type = 'available'
        else:
            type = elem.getAttribute('type')
            
        q.logger.log("[XMPPCLIENT] Presence received from '" + fromm + "' of type '" + type +"'", 5)
        
        if self.presenceReceivedCallback:
            self.presenceReceivedCallback(fromm, type)                
    
    def _message_received(self, elem):
        fromm = elem.getAttribute('from').split("@")[0]
        type = elem.getAttribute('type')
        id = elem.getAttribute('id')
        message = str(elem.children[0].children[0])
        
        q.logger.log("[XMPPCLIENT] Message '" + str(id) + "' of type '" + str(type) +"'" + "' received from '" + fromm + "'", 5)
        
        if self.messageReceivedCallback:
            self.messageReceivedCallback(fromm, type, id, message)
    
    def _init_failed(self, failure):
        self._disconnected('Init failed ' + str(failure))
    
    def _end_stream(self, xmlstream):
        self._disconnected('Stream ended: connection lost')
        
    def _watchdog_timeout(self):
        self.connector.disconnect()
        self._disconnected('Connection timed out')

    def _disconnected(self, reason):
        self.status = 'DISCONNECTED'
        q.logger.log("[XMPPCLIENT] Disconnected: " + reason, 4)
        
        if self.disconnectedCallback:
            self.disconnectedCallback(reason)

class NotConnectedException(Exception):
    def __init__(self):
        Exception.__init__(self)
