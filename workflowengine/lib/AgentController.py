from pymonkey import q
import yaml
from concurrence import Tasklet, Message
from workflowengine.XMPPClient import XMPPClient
from workflowengine.Exceptions import AgentNotAvailableException, TimeOutException, ScriptFailedException
import base64

class MSG_XMPP_SEND_MESSAGE(Message): pass
class MSG_XMPP_SEND_PRESENCE(Message): pass
class MSG_XMPP_SENT(Message): pass
class MSG_XMPP_ERROR(Message): pass

class MSG_JOB_DONE(Message): pass
class MSG_JOB_TIMEOUT(Message): pass

class AgentControllerTask:

    def __init__(self, agentcontrollerid, xmppserver, hostname, password):
        self.__xmppclient = XMPPClient(agentcontrollerid, xmppserver, hostname, password)
        self.__sending_tasklet = None
        self.__receiving_tasklet = None
        self.__agent_controller = None

    def start(self):
        self.__sending_tasklet = Tasklet.new(self.__sender)()

    def __sender(self):
        # Start the xmppclient: not done during initialization because failed connection would block initialization
        self.__xmppclient.start()
        # Started: start the receiving tasklet
        self.__receiving_tasklet = Tasklet.new(self.__receiver)()
        # Wait for messages for the XMPPClient
        for msg, args, kwargs in Tasklet.receive():
            if msg.match(MSG_XMPP_SEND_MESSAGE):
                try:
                    self.__xmppclient.sendMessage(args[1], args[2], args[3], args[4])
                except Exception, e:
                    MSG_XMPP_ERROR.send(args[0])(e)
                else:
                    MSG_XMPP_SENT.send(args[0])()
            elif msg.match(MSG_XMPP_SEND_PRESENCE):
                try:
                    self.__xmppclient.sendPresence(args[1], args[2])
                except Exception, e:
                    MSG_XMPP_ERROR.send(args[0])(e)
                else:
                    MSG_XMPP_SENT.send(args[0])()

    def __receiver(self):
        # Get the messages received by the XMPPClient and pass them to the agent controller
        for element in self.__xmppclient.receive():
            try:
                if element['type'] == 'presence':
                    self.__agent_controller and self.__agent_controller._presence_received(element['from'], element['presence_type'])
                elif element['type'] == 'message':
                    self.__agent_controller and self.__agent_controller._message_received(element['from'], element['message_type'], element['id'], element['message'])
                elif element['type'] == 'disconnected':
                    self.__agent_controller and self.__agent_controller._disconnected()
            except Exception, ex:
                q.logger.log('AGENTCONTROLLER: Failed to handle received message! message: %s - error: %s' % (element, ex.message), 1)
                

    def connectWFLAgentController(self, wflAgentController):
        self.__agent_controller = AgentController(self.__sending_tasklet)
        wflAgentController.setAgentController(self.__agent_controller)


class AgentController:

    def __init__(self, send_tasklet):
        self.__send_tasklet = send_tasklet
        self.__presenceList = PrecenseList()
        self.__jobQueue = JobQueue()

    def executeScript(self, agentguid, jobguid, scriptpath, params):
        '''
        Execute a script with a set of params on a certain agent and wait for an answer from the agent.

        @raise IOError: if the scriptpath can't be read
        @raise TimeOutException: if the agent doesn't respond within the timeout
        @raise AgentNotAvailableException: if the agent is not available when starting the script, the script is not started

        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.__presenceList.is_available(agentguid):
            raise AgentNotAvailableException(agentguid)

        file = open(scriptpath, 'r')
        script = file.read()
        file.close()

        timeout = q.workflowengine.jobmanager.getMaxduration(jobguid)

        script = base64.encodestring(script)

        yaml_message = yaml.dump({'params':params, 'script':script})
        self.__jobQueue.start(jobguid, agentguid, params, scriptpath)
        self.__sendMessage(agentguid, 'start', jobguid, yaml_message)
        return self.__waitForScript(agentguid, jobguid, timeout)

    def stopScript(self, agentguid, jobguid, timeout):
        '''
        Stop a script running on an agent with a certain jobguid and wait for an answer from the agent.
        @param timeout: the number of seconds to wait before raising a TimeOutException
        @raise TimeOutException: if the agent doesn't respond within the timeout
        @raise AgentNotAvailableException: if the agent is not available when stopping the script, the script is considered dead by the controller
        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.__presenceList.is_available(agentguid):
            self.__jobQueue.died(jobguid, agentguid, 1, "Agent was down when script was stopped by user.")
            raise AgentNotAvailableException(agentguid)

        self.__sendMessage(agentguid, 'stop', jobguid)
        return self.__waitForScript(agentguid, jobguid, timeout)

    def killScript(self, agentguid, jobguid, timeout):
        '''
        Kill a script running on an agent with a certain jobguid and wait for an answer from the agent.
        @param timeout: the number of seconds to wait before raising a TimeOutException
        @raise AgentNotAvailableException: if the agent is not available when stopping the script, the script is considered dead by the controller
        @return: a dict with keys: 'params' and 'error', if 'error' == True: dict also contains 'errorcode' and 'erroroutput'
        '''
        if not self.__presenceList.is_available(agentguid):
            self.__jobQueue.died(jobguid, agentguid, 1, "Agent was down when script was killed by user.")
            raise AgentNotAvailableException(agentguid)

        self.__sendMessage(agentguid, 'kill', jobguid)
        return self.__waitForScript(agentguid, jobguid, timeout)

    def listAvailableAgents(self):
        '''
        @return: list containing the agentguids of the available agents
        '''
        return self.__presenceList.list_available()

    def listRunningScripts(self):
        '''
        @return: list containing the jobguids of the running scripts
        '''
        return self.__jobQueue.listRunningScripts()

    def __waitForScript(self, agentguid, jobguid, timeout):
        '''
        This method waits until the agent returns the updated params.
        If the timeout (in seconds) is reached, a TimeOutException will be thrown.
        '''
        try:
            self.__jobQueue.waitForJobToFinish(jobguid, timeout)
        except TimeOutException:
            raise
        else:
            acjob = self.__jobQueue.removeJob(jobguid)
            if acjob.failed is True:
                raise ScriptFailedException(jobguid, agentguid, acjob.scriptpath, acjob.errorcode, acjob.erroroutput)
            else:
                return acjob.params

    def _presence_received(self, agentguid, type):
        if type == 'available':
            self.__presenceList.set_available(agentguid)
        elif type == 'unavailable':
            self.__presenceList.set_unavailable(agentguid)
        elif type == 'subscribe':
            # Subscribe everybody
            self.__sendPresence(agentguid, 'subscribed')
            self.__sendPresence(agentguid, 'subscribe')
        elif type == 'subscribed':
            pass

    def _message_received(self, agentguid, type, jobguid, message):
        if type == 'agent_done':
            return_params = yaml.load(message)
            self.__jobQueue.done(jobguid, agentguid, return_params)
        elif type == 'agent_error':
            return_object = yaml.load(message)
            errorcode = return_object['errorcode']
            erroroutput = return_object['erroroutput']
            self.__jobQueue.died(jobguid, agentguid, errorcode, erroroutput)
        elif type == 'agent_log':
            message_object = yaml.load(message)
            q.workflowengine.jobmanager.appendJobLog(jobguid, message_object['message'].encode(), message_object['level'], agentguid.encode())

    def _disconnected(self):
        self.__presenceList.clear()

    def __sendMessage(self, to, type, id, message=' '):
        MSG_XMPP_SEND_MESSAGE.send(self.__send_tasklet)(Tasklet.current(), to, type, id, message)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_XMPP_ERROR):
            raise args[0]

    def __sendPresence(self, to=None, type=None):
        MSG_XMPP_SEND_PRESENCE.send(self.__send_tasklet)(Tasklet.current(), to, type)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_XMPP_ERROR):
            raise args[0]


class ACJob:
    def __init__(self, agentguid, running, failed, params, tasklet, scriptpath):
        self.agentguid = agentguid
        self.running = running
        self.failed = failed
        self.params = params
        self.tasklet = tasklet
        self.errorcode = 0
        self.erroroutput = None
        self.scriptpath = scriptpath

class JobQueue:

    def __init__(self, ):
        # Queue is dict sorted by jobguids
        self.queue = {}

    def start(self, jobguid, agentguid, params, scriptpath):
        if jobguid in self.queue:
            raise Exception('Jobguid already in queue ' + str(jobguid))
        else:
            self.queue[jobguid] = ACJob(agentguid, True, False, params, Tasklet.current(), scriptpath)

    def done(self, jobguid, agentguid, return_params):
        if self.__checkJob(jobguid, agentguid):
            acjob = self.queue[jobguid]
            acjob.running = False
            acjob.failed = False
            acjob.params = return_params
            MSG_JOB_DONE.send(acjob.tasklet)()

    def died(self, jobguid, agentguid, errorcode, erroroutput):
        if self.__checkJob(jobguid, agentguid):
            acjob = self.queue[jobguid]
            acjob.running = False
            acjob.failed = True
            acjob.errorcode = errorcode
            acjob.erroroutput = erroroutput
            MSG_JOB_DONE.send(acjob.tasklet)()

    def waitForJobToFinish(self, jobguid, timeout):
        if timeout: timeoutTasklet = Tasklet.new(self.__timeout_tasklet)(Tasklet.current(), timeout)
        (msg, args, kwargs) = Tasklet.receive().next()
        if msg.match(MSG_JOB_DONE):
            if timeout: timeoutTasklet.jobdone = True
            return
        elif msg.match(MSG_JOB_TIMEOUT):
            job = self.queue[jobguid]
            raise TimeOutException(jobguid=jobguid, agentguid=job.agentguid, scriptpath=job.scriptpath, timeout=timeout)

    def __timeout_tasklet(self, caller, timeout):
        Tasklet.sleep(timeout)
        if not hasattr(Tasklet.current(), 'jobdone'):
            MSG_JOB_TIMEOUT.send(caller)()

    def removeJob(self, jobguid):
        return self.queue.pop(jobguid)

    def listRunningScripts(self):
        return self.queue.keys()

    def __checkJob(self, jobguid, agentguid):
        if self.queue.get(jobguid) == None:
            q.logger.log("[AGENTCONTROLLER] Received done message for unknown job '" + jobguid + "' from agent '" + agentguid + "'", 3)
            return False
        elif self.queue[jobguid].agentguid.lower() <> agentguid.lower():
            q.logger.log("[AGENTCONTROLLER] Received done message for job '" + jobguid + "' from wrong agent '" + agentguid + "' in stead of '" + self.queue[jobguid].agentguid + "'", 3)
            return False
        else:
            return True

    def __jobRunning(self, jobguid):
        return self.queue[jobguid].running


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

