from pymonkey import q
from pymonkey.config import ConfigManagementItem, ItemGroupClass

def inAppserver():
    import threading
    return hasattr(q.application, '_store') and isinstance(q.application._store, threading.local)

try:
    import stackless
except ImportError:
    stackless = None

if stackless:
    from workflowengine.WFLActionManager import WFLActionManager
    from workflowengine.WFLAgentController import WFLAgentController
    from workflowengine.DRPClient import DRPClient
    
    ActionManager = WFLActionManager
    AgentController = WFLAgentController
    Drp = DRPClient
else:
    
    class Dummy:
        pass
    
    if inAppserver():
        from workflowengine.CloudAPIActionManager import WFLActionManager
        ActionManager = WFLActionManager
    else:
        ActionManager = Dummy
    
    AgentController = Dummy
    Drp = Dummy

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


class StacklessSocketConfigItem(ConfigManagementItem):
    """
    Configuration of the StacklessSocket.
    """

    CONFIGTYPE = "StacklessSocket"
    DESCRIPTION = "StacklessSocket configuration"

    def ask(self):
        self.dialogAskString('host', 'The address of the stackless workflow engine', 'localhost')
        self.dialogAskInteger('port', 'The port of the stackless workflow engine', 9876)

StacklessSocketConfig = ItemGroupClass(StacklessSocketConfigItem)

