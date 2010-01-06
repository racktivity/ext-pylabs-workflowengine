# <License type="Sun Cloud BSD" version="2.2">
#
# Copyright (c) 2005-2009, Sun Microsystems, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or
# without modification, are permitted provided that the following
# conditions are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# 3. Neither the name Sun Microsystems, Inc. nor the names of other
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY SUN MICROSYSTEMS, INC. "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL SUN MICROSYSTEMS, INC. OR
# CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
# OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# </License>

'''PyMonkey framework central class (of which the C{q} object is an instance)'''

import sys
import inspect
import os
import imp
import random
if not (sys.platform.startswith("win32") or sys.platform.startswith("linux")  or sys.platform.startswith("darwin") or sys.platform.startswith("sunos")):
    print "PyMonkey framework is only supported on win32, darwin and linux*. Current platform is [%s]. " % (sys.platform)
    sys.exit(1)

import pymonkey

from Dirs import Dirs
from pymonkey.base.time.Time import Time
from pymonkey.base.idgenerator.IDGenerator import IDGenerator
from pymonkey.enumerators.QPackageQualityLevelType import QPackageQualityLevelType


from pymonkey.extensions.PMExtensions import PMExtensions

#We'll only lazy-load all attributes here to save memory. And launch time
_DUMMY = object()
class PYMonkeyContainerAttributeDescriptor(object):
    def __init__(self, name, generator):
        self.name = name
        self.generator = generator

    def get_attribute_container(self, obj):
        if not hasattr(obj, '_lazy_attribute_container'):
            obj._lazy_attribute_container = dict()

        return obj._lazy_attribute_container

    def __get__(self, obj, type_=None):
        container = self.get_attribute_container(obj)

        ret = container.get(self.name, _DUMMY)

        if ret is _DUMMY:
            ret = self.generator()
            container[self.name] = ret

        # Overwrite class attribute (this descriptor) in instance dict
        setattr(obj, self.name, ret)
        return ret

    def __set__(self, obj, value):
        self.get_attribute_container(obj)[self.name] = value

    def __delete__(self, obj):
        raise RuntimeError('This attribute can not be deleted')


def _pymonkey_system():
    from System import System
    return System()

import time
def alog(*l,**kw):
    p=0
    if isinstance(l[-1],int):
        p=l[-1]
    if p<5:
        f=file('/tmp/t','ab')
        s=' '.join([str(i) for i in l])
        f.write('%.3f %s\r\n'%(time.time(),s))
        f.close()

def _pymonkey_logger():
    from pymonkey.logging.LogHandler import LogHandler
    l = LogHandler()
    l._log=alog
    l.log=alog
    return l


def _pymonkey_eventhandler():
    from pymonkey.logging.EventHandler import EventHandler
    return EventHandler()

def _pymonkey_application():
    from Application import Application
    return Application()

def _pymonkey_vars():
    from pymonkey.Vars import Vars
    return Vars

def _pymonkey_platform():
    from pymonkey.enumerators.PlatformType import PlatformType
    return PlatformType.findPlatformType()

def _pymonkey_cmdb():
    from pymonkey.cmdb import CMDB
    return CMDB()

def _pymonkey_action():
    from pymonkey.action import ActionController
    return ActionController()

def _pymonkey_console():
    from pymonkey.console import Console
    return Console()

def _pymonkey_extensions():
    import pymonkey.extensions.management
    return pymonkey.extensions.management

def _pymonkey_qshellconfig():
    from pymonkey.qshellconfig.QShellConfig import QShellConfig
    return QShellConfig()

def _pymonkey_tools():
    from pymonkey.Tools import Tools
    return Tools

def _pymonkey_clients():
    from pymonkey.clients.Clients import Clients
    return Clients()

def _pymonkey_codetools():
    from pymonkey.codetools.CodeTools import CodeTools
    return CodeTools()

def _pymonkey_enumerators():
    from pymonkey.baseclasses.BaseEnumeration import enumerations
    return enumerations

def _pymonkey_config():
    from pymonkey.config.QConfig import QConfig
    return QConfig()

def _pymonkey_gui():
    from pymonkey.gui.Gui import Gui
    return Gui()

def _pymonkey_debugger():
    from pymonkey.debugger import QHook
    return QHook()

def _pymonkey_qpackages():
    from pymonkey.qpackages.client.QPackageManagement import QPackageManagement
    return QPackageManagement()

def _pymonkey_qpackagetools():
    from pymonkey.qpackages.client.QPackageTools import QPackageTools
    return QPackageTools()

def _pymonkey_qpackageserver():
    from pymonkey.qpackages.server.ServerManagement import ServerManagement
    return ServerManagement()

class PYMonkey:
    '''Central PyMonkey framework class, of which C{q} is an instance'''
    # Construct the singleton objects
    system = PYMonkeyContainerAttributeDescriptor('system',_pymonkey_system)
    '''Accessor to system methods'''

    logger = PYMonkeyContainerAttributeDescriptor('logger',_pymonkey_logger)
    '''Accessor to logging methods'''

    eventhandler = PYMonkeyContainerAttributeDescriptor('eventhandler',_pymonkey_eventhandler)
    '''Accessor to event handling methods'''

    application = PYMonkeyContainerAttributeDescriptor('application',_pymonkey_application)
    '''Accessor to application methods'''

    #TODO Somehow the PYMonkeyContainerAttributeDescriptor trick can't be used
    #on the dirs attribute (test.system.test_fs.TestDirs.test_dir fails).
    dirs = Dirs()
    '''Accessor to directory configuration'''

    vars = PYMonkeyContainerAttributeDescriptor('vars',
            _pymonkey_vars)
    '''Accessor to shared variables'''

    platform = PYMonkeyContainerAttributeDescriptor('platform',
            _pymonkey_platform)
    '''Accessor to current platform information'''

    cmdb = PYMonkeyContainerAttributeDescriptor('cmdb', _pymonkey_cmdb)
    '''Accessor to the PyMonkey CMDB subsystem'''

    action = PYMonkeyContainerAttributeDescriptor('action',
            _pymonkey_action)
    '''Accessor to the PyMonkey action methods'''
    
    console = PYMonkeyContainerAttributeDescriptor('console',
            _pymonkey_console)
    ''' Accessor to the PYmonkey console methods'''

    extensions = PYMonkeyContainerAttributeDescriptor('extensions',
            _pymonkey_extensions)
    '''Extension management methods'''

    qshellconfig = PYMonkeyContainerAttributeDescriptor('qshellconfig',
            _pymonkey_qshellconfig)

    _extensionsInited = False
    '''Whether extensions are initialized'''
    _pmExtensions = None
    '''List of discovered extensions'''

    tools = PYMonkeyContainerAttributeDescriptor('tools', _pymonkey_tools)
    '''Accessor to PyMonkey tools'''

    clients = PYMonkeyContainerAttributeDescriptor('clients',
            _pymonkey_clients)
    ''' Accessor to client applications '''

    enumerators = PYMonkeyContainerAttributeDescriptor('enumerators',
            _pymonkey_enumerators)
    '''Accessor to all registered enumeration types'''

    config = PYMonkeyContainerAttributeDescriptor('config',
            _pymonkey_config)

    codetools = PYMonkeyContainerAttributeDescriptor('codetools', _pymonkey_codetools)

    gui = PYMonkeyContainerAttributeDescriptor('gui',
            _pymonkey_gui)

    debugger = PYMonkeyContainerAttributeDescriptor('debugger',
                                                    _pymonkey_debugger)

    qpackages = PYMonkeyContainerAttributeDescriptor('qpackages',
                                                     _pymonkey_qpackages)

    qpackagetools = PYMonkeyContainerAttributeDescriptor('qpackagetools',
                                                         _pymonkey_qpackagetools)

    qpackageserver = PYMonkeyContainerAttributeDescriptor('qpackageserver',
                                                          _pymonkey_qpackageserver)


    def __init__(self):
        q = getattr(pymonkey, 'q', None)
        if q and q is not self:
            raise RuntimeError('Creating a second PYMonkey instance')
        self._init_called = False
        self._init_final_called = False


    @staticmethod
    def getTaskletEngine(path=None):
        '''Get a tasklet engine instance

        If a C{path} is provided, this is passed to the C{addFromPath} method of
        the new tasklet engine.

        @param path: Path passed to addFromPath
        @type path: string

        @return: A tasklet engine
        @rtype: L{pymonkey.tasklets.TaskletsEngine}
        '''
        from pymonkey.tasklets import TaskletsEngine

        engine = TaskletsEngine()

        if path:
            engine.addFromPath(path)

        return engine


    def init(self):
        """
        Core pymonkey functionality.
        You cannot use q.dirs in here since it is not yet configured
        """
        if self._init_called:
            raise RuntimeError('q.init already called. Are you re-importing '
                                'pymonkey.InitBase*?')

        #We want to do this asap
        self.basetype = pymonkey.pmtypes.register_types()
        class _dummy: pass
        d = _dummy()
        setattr(d, 'time', Time())
        setattr(d, 'idgenerator', IDGenerator())
        self.base = d

        self._init_called = True
        pymonkey.q.logger._init()


    def init_final(self): #@remark not lin line with pymonkey code conventions
        '''Initializations which depend on other initializations should go here'''
        if self._init_final_called:
            raise RuntimeError('q.init_final already called. Are you '
                               're-importing pymonkey.InitBase*?')

        self.vars.pm_setSystemVars()

        self._initExtensionsIfNotDoneYet()
        self.qshellconfig.refresh()
        self.gui.dialog.pm_setDialogHandler()

        self._init_final_called = True


    def enablePYMonkeyTrace(self):
        '''Enable tracing in PyMonkey methods'''
        file = inspect.getfile(System)
        folder = os.path.dirname(file)
        files = os.listdir(folder)
        for f in files:
            if not f.endswith(".py"):
                continue
            if f.endswith("Logger.py"):
                continue
            if f.endswith("LogHandler.py"):
                continue
            if f.endswith("LogServer.py"):
                continue
            mod = __import__(os.path.splitext(os.path.basename(f))[0], globals(), locals(), "*")
            self.logger.logModuleUsage(mod, 10)

    def _initExtensionsIfNotDoneYet(self):
        '''Initialize PyMonkey extensions if they are not initialized yet'''
        if not self._extensionsInited:
            self._initDynamicExtensions()
            self._extensionsInited = True

    def _initDynamicExtensions(self):
        '''Initialize all extensions in self.dirs.extensionDir'''
        self.logger.log('Loading PyMonkey extensions from %s' % self.dirs.extensionsDir, 7)

        if not self.dirs.extensionsDir or not os.path.exists(self.dirs.extensionsDir):
            self.logger.log('Extension path %s does not exist, unable to load extensions' % self.dirs.extensionsDir, 6)
            return
        self._pmExtensions = PMExtensions(pymonkey.q, 'q.')
        self._pmExtensions.init()
        self._pmExtensions.findExtensions()
