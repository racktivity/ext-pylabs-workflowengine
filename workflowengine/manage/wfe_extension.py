from pymonkey.config import *

from workflowengine.WFLActionManager import WFLActionManager
from workflowengine.WFLAgentController import WFLAgentController
from workflowengine.DRPClient import DRPClient

class ActionManager(WFLActionManager):
    pass

class AgentController(WFLAgentController):
    pass

class Drp(DRPClient):
    pass

class AgentControllerConfigItem(ConfigManagementItem):
    """
    Configuration of the agentcontroller.
    """

    CONFIGTYPE = "agentcontroller"
    DESCRIPTION = "agentcontroller configuration"

    def ask(self):
        self.dialogAskString('agentcontrollerid', 'The ID of the agentcontroller', None)
        self.dialogAskString('xmppserver', 'The dns-address of the xmpp server', None)
        self.dialogAskPassword('password', 'The password for the agent on the xmpp server', None)

AgentControllerConfig = ItemGroupClass(AgentControllerConfigItem)

