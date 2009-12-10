from pymonkey import q

import random

from concurrence import Tasklet, Message
from concurrence.io import Connector
from concurrence.io.buffered import BufferedStream
from concurrence.xmpp import sasl

from xml.etree.cElementTree import Element, tostring, iterparse

class MSG_CLIENT_CONNECTED(Message): pass

class XMPPClient:

    maxDelay = 30
    initialDelay = 1.0
    factor = 2.7182818284590451
    jitter = 0.11962656472

    def __init__(self, username, server, hostname, password):
        self.__username = username
        self.__server = server
        self.__password = password
        self.__hostname = hostname

        self.__socket = None
        self.__elements = None
        self.__connected = False
        self.__delay = self.initialDelay

        self.__queuedTasklets = []

    def start(self):
        ''' Start the xmpp client, opens the connection to the server '''
        q.logger.log("[SL XMPPCLIENT] Starting the xmpp client for " + self.__username + "@" + self.__hostname, 3)
        try:
            self._connect()
        except Exception, e:
            q.logger.log("[SL XMPPCLIENT] Connection failed: " + str(e), 3)
            self._reconnect()

    def stop(self):
        ''' Stop the xmpp client '''
        q.logger.log("[SL XMPPCLIENT] Stopping the xmpp client for " + self.__username + "@" + self.__hostname, 3)
        self._close()

    def _reconnect(self):
        while not self.__connected:
            self.__delay = min(self.__delay * self.factor, self.maxDelay)
            self.__delay = random.normalvariate(self.__delay, self.__delay * self.jitter)
            q.logger.log("[SL XMPPCLIENT] Reconnecting " + self.__username + "@" + self.__hostname + " in " + str(self.__delay) + " seconds.", 3)
            Tasklet.sleep(self.__delay)
            try:
                self._connect()
            except Exception, e:
                q.logger.log("[SL XMPPCLIENT] Connection failed: " + str(e), 3)
        self.__delay = self.initialDelay

    def _connect(self):
        self.__connected = False

        self.__socket = Connector.connect((self.__server, 5222))

        q.logger.log("[SL XMPPCLIENT] Connected to server '" + self.__server + "'", 3)
        #start xml stream
        self.stream = XMPPStream(self.__socket)
        self.__elements = self.stream.elements()

        self.stream.write_start(self.__hostname)

        #perform auth handshake
        self._handshake(self.__username, self.__password, self.__hostname)

        q.logger.log("[SL XMPPCLIENT] Server '" + self.__hostname + "' authenticated user '" + self.__username + "'", 5)
        #after SASL-auth we are supposed to restart the xml stream:
        self.stream.reset()
        self.__elements = self.stream.elements()
        self.stream.write_start(self.__hostname)

        #read stream features
        element = self.__elements.next()
        if element.tag != '{http://etherx.jabber.org/streams}features':
            assert False, "expected stream features, got: %s" % element.tag

        self.stream.write_bind_request()
        element = self.__elements.next()
        if element.tag != '{jabber:client}iq':
            assert False, "expected iq, got: %s" % element.tag

        #send session request
        self.stream.write_session_request()
        element = self.__elements.next()
        if element.tag != '{jabber:client}iq':
            assert False, 'expected iq result got: %s' % element.tag

        # Connected !
        self.__connected = True
        self.sendPresence()

        # Dequeue the waiting tasklets
        for queued in self.__queuedTasklets:
            MSG_CLIENT_CONNECTED.send(queued)()


    def _handshake(self, username, password, realm):
        #perform SASL handshake
        element_features = self.__elements.next()
        if element_features.tag != '{http://etherx.jabber.org/streams}features':
            assert False, 'unexpected tag: %s expected features' % element_features.tag

        self.stream.write_auth()
        element_challenge = self.__elements.next()
        if element_challenge.tag != '{urn:ietf:params:xml:ns:xmpp-sasl}challenge':
            assert False, 'unexpected element: %s' % element_challenge.tag

        response = sasl.response(element_challenge.text, username, password, realm, 'xmpp/' + realm)
        self.stream.write_sasl_response(response)
        element = self.__elements.next()
        if element.tag == '{urn:ietf:params:xml:ns:xmpp-sasl}failure':
            q.logger.log("[SL XMPPCLIENT] Authentication of : " + self.__username + " on " + self.__hostname + " failed.", 3)
            assert False, "login failure"
        elif element.tag == '{urn:ietf:params:xml:ns:xmpp-sasl}challenge':
            pass #OK
        else:
            assert False, "unexpected element: %s" % element.tag

        self.stream.write_sasl_response()
        element = self.__elements.next()
        if element.tag != '{urn:ietf:params:xml:ns:xmpp-sasl}success':
            assert False, "error %s" % element.tag

    def _close(self):
        if not self.__socket.is_closed():
            self.stream.write_end()
            self.__socket.close()

    def sendPresence(self, to=None, type=None):
        ''' Send a presence.  The current tasklet will be queued if the xmppclient is not connected. '''
        q.logger.log("[SL XMPPCLIENT] Sending presence of type '" + str(type) +"' to " + str(to) + "'", 5)
        self.__queueTillConnected()
        if to: to = to+"@"+self.__hostname
        self.stream.write_presence(to, type)

    def sendMessage(self, to, type, id, message=' '):
        ''' Send a message. The current tasklet will be queued if the xmppclient is not connected.
        @param to: The username of the client to send the message to
        @type to: string
        @param type: The type of the message
        @type type: string
        @param id: The id of the message
        @type id: string
        @param message: The message to send
        @type message: string
        '''
        q.logger.log("[SL XMPPCLIENT] Sending message '" + str(id) + "' of type '" + str(type) +"' to " + str(to) + " for " + self.__username + "@" + self.__hostname, 5)
        self.__queueTillConnected()
        self.stream.write_message(to+"@"+self.__hostname, type, id, message)

    def __queueTillConnected(self):
        ''' Checks if the xmppclient is connected. If it is not connected, the current tasklet will be queued until a connection is established. '''
        if not self.__connected:
            self.__queuedTasklets.append(Tasklet.current())
            q.logger.log("[SL XMPPCLIENT] XMPPClient not connected: message queued", 5)
            (msg, args, kwargs) = Tasklet.receive().next()
            if msg.match(MSG_CLIENT_CONNECTED):
                return
            else:
                raise Exception("Wrong message, expected MSG_CLIENT_CONNECTED: " + str(msg))

    def receive(self):
        '''
        Generator that returns the received messages and presences.
        @return: dictionary. Key 'type' can be either 'presence', 'message' or 'disconnected'. If the type is 'presence', the dict also contains 'from' and 'presence_type'. If the type is 'message', the dict also contains 'from', 'message_type', 'id' and 'message'. If the type is 'disconnected' the dict doesn't contain any extra data.
        '''
        while True:
            try:
                for element in self.__elements:
                    if element.tag == '{jabber:client}presence':
                        fromm = element.get('from').split("@")[0]
                        type = element.get('type') or 'available'
                        q.logger.log("[SL XMPPCLIENT] Received presence from '" + fromm + "' of type '" + type + " for " + self.__username + "@" + self.__hostname, 5)
                        yield {'type':'presence', 'from':fromm, 'presence_type':type}
                    elif element.tag == '{jabber:client}message':
                        fromm = element.get('from').split("@")[0]
                        message = unescapeFromXml(element.getchildren()[0].text)
                        q.logger.log("[SL XMPPCLIENT] Received message '" + element.get('id') + "' from '" + fromm + "' of type '" + element.get('type') + " for " + self.__username + "@" + self.__hostname, 5)
                        yield {'type':'message', 'from':fromm, 'message_type':element.get('type'), 'id':element.get('id'), 'message':message}
                    else:
                        q.logger.log("[SL XMPPCLIENT] Received wrong tag: '" + str(element.tag)  + " for " + self.__username + "@" + self.__hostname, 5)
            except EOFError:
                # Connection was lost: try to reconnect !
                self.__connected = False
                q.logger.log("[SL XMPPCLIENT] Connection lost.", 3)
                self._reconnect()
                yield {'type':'disconnected'}


class XMPPStream:

    def __init__(self, stream):
        self.stream = BufferedStream(stream)
        self.reset()

    def reset(self):
        self.parser = None

    def __write_bytes(self, s):
        self.stream.writer.clear()
        self.stream.writer.write_bytes(s)
        self.stream.writer.flush()

    def write_start(self, to):
        start = "<stream:stream xmlns:stream='http://etherx.jabber.org/streams' xmlns='jabber:client' to='%s' version='1.0'>" % to
        self.__write_bytes(start)

    def write_end(self):
        self.__write_bytes("</stream:stream>")

    def write_auth(self, mechanism = 'DIGEST-MD5'):
        self.__write_bytes("<auth xmlns='urn:ietf:params:xml:ns:xmpp-sasl' mechanism='%s'/>" % mechanism)

    def write_sasl_response(self, response = ''):
        if response:
            self.__write_bytes("<response xmlns='urn:ietf:params:xml:ns:xmpp-sasl'>%s</response>" % response)
        else:
            self.__write_bytes("<response xmlns='urn:ietf:params:xml:ns:xmpp-sasl'/>")

    def write_bind_request(self):
        self.__write_bytes("<iq type='set' id='H_0'><bind xmlns='urn:ietf:params:xml:ns:xmpp-bind'/></iq>")

    def write_session_request(self):
        self.__write_bytes("<iq type='set' id='H_1'><session xmlns='urn:ietf:params:xml:ns:xmpp-session'/></iq>")

    def write_presence(self, to, type):
        attribs={}
        if to <> None: attribs['to'] =  escapeToXml(to, isattrib=1)
        if type <> None: attribs['type'] = escapeToXml(type, isattrib=1)
        presence = Element("presence", attribs)
        self.__write_bytes(tostring(presence))

    def write_message(self, to, type, id, message):
        elemToSend = Element("message", {'to':escapeToXml(to, isattrib=1), 'type':escapeToXml(type, isattrib=1), 'id':escapeToXml(id, isattrib=1)})
        body = Element('body')
        body.text = escapeToXml(message)
        elemToSend.append(body)
        self.__write_bytes(tostring(elemToSend))

    def elements(self):
        if not self.parser:
            reader = self.stream.reader
            class f(object):
                def read(self, n):
                    if reader.buffer.remaining == 0:
                        #read more data into buffer
                        reader._read_more()
                    return reader.buffer.read_bytes(min(n, reader.buffer.remaining))

            self.parser = iter(iterparse(f(), events=("start", "end")))
            self.parser.next() # Ignore the root object
            level = 0

        for event, element in self.parser:
            if event == 'start':
                level += 1
            elif event == 'end':
                level -= 1
                if level == 0:
                    yield element
            else:
                assert False, "unexpected event"

def escapeToXml(text, isattrib=0):
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    if isattrib == 1:
        text = text.replace("'", "&apos;")
        text = text.replace("\"", "&quot;")
    return text

def unescapeFromXml(text):
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&apos;", "'")
    text = text.replace("&quot;", "\"")
    text = text.replace("&amp;", "&")
    return text
