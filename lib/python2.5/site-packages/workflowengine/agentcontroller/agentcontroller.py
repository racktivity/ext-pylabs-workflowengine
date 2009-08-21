import yaml, time
from pymonkey import q, i
from twisted.internet import defer, reactor
from workflowengine.agentcontroller.xmppclient import XMPPClient

class AgentController:
    
    def __init__(self, agentcontrollerID, xmppServer, password):
        self.xmppclient = XMPPClient(agentcontrollerID, xmppServer, password)
        self.xmppclient.setMessageReceivedCallback(self._message_received)
        self.xmppclient.setPresenceReceivedCallback(self._presence_received)
        self.xmppclient.setDisconnectedCallback(self._disconnected)
        self.xmppclient.start()
        
        self.presenceList = PrecenseList()
        self.jobQueue = JobQueue()
    
    def executeScript(self, agentguid, jobguid, scriptpath, params, timeout, logger):
        '''
        Execute a script with a set of params on a certain agent and wait for an answer from the agent.
        
        @param timeout: the number of seconds to wait before raising a TimeOutException
        @param logger: the function that will be called for logging. Attributes should be: (logmessage[, level][, source]).
        
        @raise IOError: if the scriptpath can't be read
        @raise TimeOutException: if the agent doesn't respond within the timeout
        @raise AgentNotAvailableException: if the agent is not available when starting the script, the script is not started
        
        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.presenceList.is_available(agentguid):
            raise AgentNotAvailableException(agentguid)
        
        file = open(scriptpath, 'r')
        script = file.read()
        file.close()
        
        yaml_message = yaml.dump({'params':params, 'script':script})
        
        self.jobQueue.start(jobguid, agentguid, params, logger)
        self.xmppclient.sendMessage(agentguid, 'start', jobguid, yaml_message)
        return self.__waitForScript(agentguid, jobguid, timeout)
    
    def stopScript(self, agentguid, jobguid, timeout):
        '''
        Stop a script running on an agent with a certain jobguid and wait for an answer from the agent.
        @param timeout: the number of seconds to wait before raising a TimeOutException
        @raise TimeOutException: if the agent doesn't respond within the timeout
        @raise AgentNotAvailableException: if the agent is not available when stopping the script, the script is considered dead by the controller
        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.presenceList.is_available(agentguid):
            self.jobQueue.removeJob(jobguid)
            raise AgentNotAvailableException(agentguid)
        
        self.xmppclient.sendMessage(agentguid, 'stop', jobguid)
        return self.__waitForScript(agentguid, jobguid, timeout)
    
    def killScript(self, agentguid, jobguid, timeout):
        '''
        Kill a script running on an agent with a certain jobguid and wait for an answer from the agent.
        @param timeout: the number of seconds to wait before raising a TimeOutException
        @raise AgentNotAvailableException: if the agent is not available when stopping the script, the script is considered dead by the controller
        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.presenceList.is_available(agentguid):
            self.jobQueue.removeJob(jobguid)
            raise AgentNotAvailableException(agentguid)
        
        self.xmppclient.sendMessage(agentguid, 'kill', jobguid)
        return self.__waitForScript(agentguid, jobguid, timeout)
    
    def listAvailableAgents(self):
        '''
        @return: list containing the agentguids of the available agents 
        '''
        return self.presenceList.list_available()
    
    def listRunningScript(self):
        '''
        @return: list containing the jobguids of the running scripts
        '''
        return self.jobQueue.listRunningScript()
    
    def __waitForScript(self, agentguid, jobguid, timeout):
        '''
        This method waits until the agent returns the updated params.
        If the timeout (in seconds) is reached, a TimeOutException will be thrown.
        '''
        try:
            self.jobQueue.waitForJobToFinish(jobguid, timeout)
        except TimeOutException:
            raise
        else:
            ret = self.jobQueue.getJobInfo(jobguid)
            self.jobQueue.removeJob(jobguid)
            return ret
    
    def _presence_received(self, agentguid, type):
        if type == 'available':
            self.presenceList.set_available(agentguid)
        elif type == 'unavailable':
            self.presenceList.set_unavailable(agentguid)
        elif type == 'subscribe':
            # Subscribe everybody
            self.xmppclient.sendPresence(agentguid, 'subscribed')
            self.xmppclient.sendPresence(agentguid, 'subscribe')
        elif type == 'subscribed':
            pass
    
    def _message_received(self, agentguid, type, jobguid, message):
        if type == 'agent_done':
            return_params = yaml.load(message)
            self.jobQueue.done(jobguid, agentguid, return_params)
        elif type == 'agent_error':
            return_object = yaml.load(message)
            errorcode = return_object['errorcode']
            erroroutput = return_object['erroroutput']
            self.jobQueue.died(jobguid, agentguid, errorcode, erroroutput)
        elif type == 'agent_log':
            message_ojbect = yaml.load(message)
            self._log_external(agentguid, jobguid, message_ojbect['level'], message_ojbect['message'])
    
    def _log_external(self, agentguid, jobguid, level, message):
        q.logger.log("[AGENTCONTROLLER] Log from agent '" + agentguid + "' for job '" + jobguid + "' with level '" + str(level) + "' : " + message, 6)
        reactor.callInThread(self.jobQueue.getLogger(jobguid), message.encode(), level=level, source=agentguid.encode())

    def _disconnected(self, reason):
        self.presenceList.clear()


class JobQueue:
    
    def __init__(self, ):
        # Queue is dict sorted by jobguids
        self.queue = {}

    def start(self, jobguid, agentguid, params, logger):
        if jobguid in self.queue:
            raise Exception('Jobguid already in queue ' + str(jobguid))
        
        running = True
        failed = False
        self.queue[jobguid] = [agentguid, running, failed, params, logger]
    
    def done(self, jobguid, agentguid, return_params):
        if self.__checkJob(jobguid, agentguid):
            self.queue[jobguid] = [agentguid, False, False, return_params, self.queue[jobguid][4]]
    
    def died(self, jobguid, agentguid, errorcode, erroroutput):
        if self.__checkJob(jobguid, agentguid):
            self.queue[jobguid] = [agentguid, False, True, self.queue[jobguid][3], self.queue[jobguid][4], errorcode, erroroutput]
    
    def waitForJobToFinish(self, jobguid, timeout):
        timeout = timeout and timeout * 10
        i = 0
        while self.__jobRunning(jobguid) and (not timeout or i <> timeout):
            time.sleep(0.1)
            i += 1
        if self.__jobRunning(jobguid):
            raise TimeOutException()
        else:
            return 
    
    def getJobInfo(self, jobguid):
        entry = self.queue[jobguid]
        ret = {'params' : entry[3], 'error' : entry[2]}
        if entry[2]:
            ret['errorcode'] = entry[5]
            ret['erroroutput'] = entry[6]
        return ret
    
    def getLogger(self, jobguid):
        return self.queue[jobguid][4]
    
    def removeJob(self, jobguid):
        return self.queue.pop(jobguid)
    
    def listRunningScript(self):
        return self.queue.keys()
    
    def __checkJob(self, jobguid, agentguid):
        if self.queue.get(jobguid) == None:
            q.logger.log("[AGENTCONTROLLER] Received done message for unknown job '" + jobguid + "' from agent '" + agentguid + "'", 4)
            return False
        elif self.queue[jobguid][0] <> agentguid:
            q.logger.log("[AGENTCONTROLLER] Received done message for job '" + jobguid + "' from wrong agent '" + agentguid + "' in stead of '" + self.queue[jobguid][0] + "'", 4)
            return False
        else:
            return True
    
    def __jobRunning(self, jobguid):
        return self.queue[jobguid][1]


class PrecenseList:
    
    def __init__(self):
        self.presence = {}
    
    def set_available(self, agentguid):
        q.logger.log("[AGENTCONTROLLER] Agent with guid '" + str(agentguid) + "' available", 5)
        self.presence[agentguid] = True
    
    def set_unavailable(self, agentguid):
        q.logger.log("[AGENTCONTROLLER] Agent with guid '" + str(agentguid) + "' unavailable", 5)
        self.presence[agentguid] = False
    
    def clear(self):
        q.logger.log("[AGENTCONTROLLER] Clearing the presence list", 5)
        self.presence = {}

    def is_available(self, agentguid):
        if not agentguid in self.presence:
            return False
        else:
            return self.presence[agentguid]

    def list_available(self):
        return self.presence
        
class TimeOutException(Exception):
    def __init__(self):
        Exception.__init__(self, "Timeout occurred")
        
class AgentNotAvailableException(Exception):
    def __init__(self, agentguid):
        Exception.__init__(self, "Agent with guid '"+agentguid+"' is not available.")
        