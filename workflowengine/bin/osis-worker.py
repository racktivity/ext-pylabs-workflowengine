from pymonkey.InitBaseCore import q, i
import sys, yaml

from osis import init
from osis.server.base import BaseServer
from osis.server.exceptions import ObjectNotFoundException

from amqplib import client_0_8 as amqp
from workflowengine.QueueInfrastructure import AMQPAbstraction, QueueInfrastructure

#TODO We should probably move the tasklets
tasklet_path = '/opt/qbase3/apps/applicationserver/services/osis_service/tasklets'

def initialize_osis():
    osisDir = q.system.fs.joinPaths(q.dirs.baseDir, 'libexec', 'osis')
    q.system.fs.createDir(osisDir)
    init(osisDir)

def initialize_amqp():
    #TODO Read the RabbitMQ stuff from a config
    connection = amqp.Connection(host="localhost:5672", userid="guest", password="guest", virtual_host="/", insist=False, lowdelay=True)
    channel = connection.channel()
    
    # Setup the basic infrastructure
    amqpAbstraction = AMQPAbstraction(channel.queue_declare, channel.exchange_declare, channel.queue_bind)
    QueueInfrastructure(amqpAbstraction).createBasicInfrastructure()
    
    return channel

def main():
    initialize_osis()
    osis = OsisOverQueue(tasklet_path=tasklet_path)
    
    try:
        channel = initialize_amqp()
    except IOError:
        #This error is raised if the connection was refused or abruptly closed
        sys.exit(-1)
    except amqp.exceptions.AMQPException:
        #This exception is raised if the connection was closed correctly
        sys.exit(-1)
    else:
        
        def receive(msg):
            data = yaml.load(msg.body)
            result = None
            if data['action'] is 'get':
                result = osis.get(data['type'], data['guid'], data['serializer'])
            elif data['action'] is 'get_version':
                result = osis.get_version(data['type'], data['guid'], data['version'], data['serializer'])
            elif data['action'] is 'runQuery':
                result = osis.runQuery(data['query'])
            elif data['action'] is 'delete':
                result = osis.delete(data['type'], data['guid'])
            elif data['action'] is 'delete_version':
                result = osis.delete_version(data['type'], data['guid'], data['version'])
            elif data['action'] is 'put':
                result = osis.put(data['type'], data['data'], data['serializer'])
            elif data['action'] is 'find':
                result = osis.find(data['type'], data['filter'], data['view'])
            elif data['action'] is 'findAsView':
                result = osis.findAsView(data['type'], data['filter'], data['view'])
            
            ret = yaml.dump({'id':data['id'], 'return':result}) #TODO If an exception happens, we have to send it, example: main.py.
            channel.basic_publish(amqp.Message(ret), exchange=QueueInfrastructure.WFE_OSIS_RETURN_EXCHANGE, routing_key=data['return_queue_guid'])
        
        channel.basic_consume(queue=QueueInfrastructure.WFE_OSIS_QUEUE, no_ack=True, callback=receive, consumer_tag="osis_tag")
        while True:
            channel.wait()
        channel.basic_cancel("osis_tag")
        channel.close()


class OsisOverQueue(BaseServer):
    
    def __init__(self, tasklet_path=None):
        BaseServer.__init__(self, tasklet_path)

    def get(self, objectType, guid, serializer):
        data = BaseServer.get(self, objectType, guid, serializer)
        return base64.encodestring(data)

    def get_version(self, objectType, guid, version, serializer):
        data = BaseServer.get_version(self, objectType, guid, version, serializer)
        return base64.encodestring(data)

    def runQuery(self, query):
        return BaseServer.runQuery(self, query)

    def delete(self, objectType, guid):
        return BaseServer.delete(self, objectType, guid)
        
    def delete_version(self, objectType, guid, version):
        return BaseServer.delete_version(self, objectType, guid, version)
    
    def put(self, objectType, data, serializer):
        data = base64.decodestring(data)
        BaseServer.put(self, objectType, data, serializer)
        return True

    def find(self, objectType, filters, view=''):
        return BaseServer.find(self, objectType, filters, view)

    def findAsView(self, objectType, filters, view):
        return BaseServer.findAsView(self, objectType, filters, view)

if __name__=='__main__':
    main()
    

