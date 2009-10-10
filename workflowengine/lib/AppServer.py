from concurrence.http import WSGIServer

class Serializer:
    serializers = dict()    

    def __init__(self, serializeFunction, deserializeFunction):
        self.serialize = serializeFunction
        self.deserialize = deserializeFunction

    @classmethod
    def addSerializer(cls, name, serializeFunction, deserializeFunction):
        cls.serializers[name] = Serializer(serializeFunction, deserializeFunction)

    @classmethod
    def getSerializer(cls, name):
        return cls.serializers.get(name)

class CallHandler:
    handlers = dict()

    def __init__(self, handler):
        self.handle = handler

    @classmethod
    def addCallHandler(cls, name, handler):
        cls.handlers[name] = CallHandler(handler)

    @classmethod
    def getCallHandler(cls, name):
        return cls.handlers.get(name)

def http_request_handler(environ, start_response):
    def respond(errorcodemessage, message=[]):
        start_response(errorcodemessage, [])
        return message

    if environ['REQUEST_METHOD'] == 'POST' and environ['PATH_INFO'] == '/' and 'CONTENT_TYPE' in environ:
        serializer = Serializer.getSerializer(environ['CONTENT_TYPE'])
        if serializer is not None:
            length = environ.get('CONTENT_LENGTH') or 0
            input_string = environ['wsgi.input'].read(length)
            try: 
                input = serializer.deserialize(input_string)
            except:
                return respond("500 DESERIALIZATION FAILED.")
            else:
                if 'call'in input:
                    callhandler = CallHandler.getCallHandler(input.pop('call'))
                    if callhandler is not None:
                        result = callhandler.handle(input)
                        return respond("200 OK", [ serializer.serialize(result) ])
                    else:
                        return respond("500 NO HANDLER FOUND FOR THIS CALL.")
                else:
                    return respond("500 KEY 'call' NOT FOUND AFTER DESERIALIZATION")

        else:
            return respond("501 SERIALIZER NOT IMPLEMENTED")
    else:
        return respond("500 ONLY POST TO / IS SUPPORTED, CONTENT_TYPE HAS TO BE PROVIDED")


class AppServerTask:

    def __init__(self, port):
        self.port = port
    
    @classmethod
    def addSerializer(cls, name, serializeFunction, deserializeFunction):
        Serializer.addSerializer(name, serializeFunction, deserializeFunction)
    
    @classmethod    
    def addCallHandler(cls, name, handler):
        CallHandler.addCallHandler(name, handler)
    
    def start(self):
        server = WSGIServer(http_request_handler)
        server.serve(('', self.port)) 
