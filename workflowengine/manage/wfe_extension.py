from pymonkey import q
from pymonkey.config import ConfigManagementItem, ItemGroupClass

def inAppserver():
    import threading
    return hasattr(q.application, '_store') and isinstance(q.application._store, threading.local)

def inWFE():
    return hasattr(q.application, 'appname') and q.application.appname == 'workflowengine'

if inWFE():
    from workflowengine.WFLActionManager import WFLActionManager
    from workflowengine.WFLAgentController import WFLAgentController
    from workflowengine.WFLJobManager import WFLJobManager

    ActionManager = WFLActionManager
    AgentController = WFLAgentController
    JobManager = WFLJobManager

else:

    class Dummy:
        pass

    if inAppserver():
        from workflowengine.CloudAPIActionManager import WFLActionManager
        ActionManager = WFLActionManager
    else:
        ActionManager = Dummy

    JobManager = Dummy
    AgentController = Dummy

#from workflowengine.QshellDRPClient import DRPClient
Drp = q.drp


class WFLConfigItem(ConfigManagementItem):
    """
    Configuration of the Workflowengine.
    """

    CONFIGTYPE = "Workflowengine"
    DESCRIPTION = "Workflowengine configuration"

    def ask(self):
        self.dialogAskInteger('port', 'The port of workflowengine socket', 9876)
        # osis_address: doesn't work with localhost in stead of 127.0.0.1. Due to ConcurrenceSocket limitations.
        self.dialogAskString('osis_address', 'The address of the applicationserver running the OSIS service', 'http://127.0.0.1:8888')
        self.dialogAskString('osis_service', 'The name of the OSIS service', 'osis_service')
        self.dialogAskString('xmppserver', 'The DNS address of the XMPP server', None)
        self.dialogAskString('agentcontrollerguid', 'The agentguid of the agentcontroller', None)
        self.dialogAskPassword('password', 'The password of the agentcontroller on the XMPP server', None)

WFLConfig = ItemGroupClass(WFLConfigItem)

