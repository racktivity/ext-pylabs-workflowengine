import urlparse
import xmlrpclib

import concurrence.http

from osis.client.xmlrpc import XMLRPCTransport

class ConcurrenceTransport(xmlrpclib.Transport):

    def make_connection(self, host):
        connection = concurrence.http.HTTPConnection()
        url = urlparse.urlparse('http://%s' % host)
        connection.connect((url.hostname, url.port))
        return connection

    def send_host(self, connection, host):
        connection.host = host

    def send_request(self, connection, handler, request_body):
        connection.post(handler, request_body, connection.host)

    def request(self, host, handler, request_body, verbose=0):

        connection = self.make_connection(host)
        chost, self._extra_headers, x509 = self.get_host_info(host)

        request =  connection.post(handler, request_body, host)
        request.headers.extend(self._extra_headers)
        response = connection.perform(request)

        if response.status_code != 200:
            raise xmlrpclib.ProtocolError(
                host + handler,
                response.status, response.reason,
                response.msg
                )

        p, u = self.getparser()
        p.feed(response.body)
        p.close()

        return u.close()

class ConcurrenceOsisXMLRPCTransport(XMLRPCTransport):
     def __init__(self, uri, service_name=None):
        '''Initialize a new XMLRPC transport

        @param uri: URI of the XMLRPC server
        @type uri: string
        @param service_name: Name of the service endpoint (if applicable)
        @type service_name: string
        '''

        c_transport = ConcurrenceTransport()
        self.proxy = xmlrpclib.ServerProxy(uri,transport=c_transport, allow_none=True)

        if service_name:
            self.proxy = getattr(self.proxy, service_name)
