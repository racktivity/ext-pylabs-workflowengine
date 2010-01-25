from pymonkey import q
import traceback

class DebugInterface():

    def __init__(self, socket_task):
        self.__socket_task = socket_task

    def handleMessage(self, data):
        if data['action'] == 'heartbeat':
            self.heartbeat()
        elif data['action'] ==  'kill_job':
            self.kill_job(data['jobguid'])

    def heartbeat(self):
        self.__socket_task.sendData({"action":"heartbeat", "reply":"alive"})

    def kill_job(self, jobguid):
        try:
            result = q.workflowengine.jobmanager.killJob(jobguid)
        except:
            self.__socket_task.sendData({"action":"kill_job", "reply":"failed", "message":traceback.format_exc()})
        else:
            self.__socket_task.sendData({"action":"kill_job", "reply":"success"})

