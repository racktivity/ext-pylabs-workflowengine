__author__   = 'aserver'
__tags__     = 'configure',
__priority__ = 1

def match(q, i, params, tags):
    return True

def main(q, i, params, tags):
    prompt=False
    configFile=q.system.fs.joinPaths(q.dirs.cfgDir,'qconfig','Workflowengine.cfg')

    if q.system.fs.exists(configFile):
        iniFile=q.tools.inifile.open(configFile)
    else:
        iniFile=q.tools.inifile.new(configFile)

    if not iniFile.checkSection('main'):
        iniFile.addSection('main')

    if not iniFile.checkParam('main', 'osis_address'):
        value = q.gui.dialog.askString('\nPlease enter the address of the applicationserver running the OSIS service',defaultValue='http://127.0.0.1:8888')
        iniFile.addParam('main', 'osis_address', value)

    if not iniFile.checkParam('main', 'osis_service'):
        value = q.gui.dialog.askString('Please enter the name of the osis service',defaultValue='osis_service')
        iniFile.addParam('main', 'osis_service', value)

    if not iniFile.checkParam('main', 'xmppserver'):
        value = q.gui.dialog.askString('Please enter the FQDN of the XMPP server',defaultValue='dmachine.office.aserver.com')
        iniFile.addParam('main', 'xmppserver', value)

    if not iniFile.checkParam('main', 'agentcontrollerguid'):
        value = q.gui.dialog.askString('Please enter the agentcontrollerguid',defaultValue='agentcontroller')
        iniFile.addParam('main', 'agentcontrollerguid', value)

    if not iniFile.checkParam('main', 'port'):
        value = q.gui.dialog.askInteger('Please enter the workflowengine port',defaultValue=9876)
        iniFile.addParam('main', 'port', value)

    if not iniFile.checkParam('main', 'password'):
        value = q.gui.dialog.askPassword('Please enter the workflowengine password',defaultValue='test')
        iniFile.addParam('main', 'password', value)

    iniFile.write(configFile)

    #Starting the workflowengine
    q.manage.workflowengine.start()

    q.manage.ejabberd.start()

    ##register agentcontroller
    config = i.config.workflowengine.getConfig('main')
    if not config['agentcontrollerguid'] in q.manage.ejabberd.cmdb.users:
        q.manage.ejabberd.startChanges()
        q.manage.ejabberd.cmdb.addUser(config['agentcontrollerguid'], config['xmppserver'], config['password'])
        q.manage.ejabberd.cmdb.save()
        q.manage.ejabberd.applyConfig()
