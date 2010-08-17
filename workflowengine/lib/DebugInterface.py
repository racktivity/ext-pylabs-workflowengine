from pymonkey import q
import traceback

class DebugInterface():

    def __init__(self, socket_task):
        self.__socket_task = socket_task

    def handleMessage(self, data, connection):
        if data['action'] == 'heartbeat':
            self.heartbeat(connection)
        elif data['action'] ==  'kill_job':
            self.kill_job(data['jobguid'], connection)

    def heartbeat(self, connection):
        connection.sendData({"action":"heartbeat", "reply":"alive"})

    def kill_job(self, jobguid, connection):
        try:
            result = q.workflowengine.jobmanager.killJob(jobguid)
        except:
            connection.sendData({"action":"kill_job", "reply":"failed", "message":traceback.format_exc()})
        else:
            connection.sendData({"action":"kill_job", "reply":"success"})

