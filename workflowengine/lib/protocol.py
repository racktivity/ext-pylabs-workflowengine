import json

class RpcMessage(object):
    '''RPC Object based on q.messagehandler.rpcmessage for API compatibility'''
    def __init__(self, application=None, domain=None, category=None, \
            methodname=None, params=None, login=None, passwd=None, 
            messageid=None, returnqueue=None):
        
        self.application = application
        self.domain = domain
        self.category = category
        self.methodname = methodname
        self.params = params
        
        self.login = login
        self.passwd = passwd
        
        self.messageid = messageid
        self.returnqueue = returnqueue

def encode_message(msg):
    '''Poor man's encoder'''
    return json.dumps(msg.__dict__)

def decode_message(data): 
    '''Poor man's decoder'''
    return RpcMessage(**json.loads(data))