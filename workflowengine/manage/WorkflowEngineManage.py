from pylabs import q
import signal, time
import yaml
import socket

class WorkflowEngineManage:
    #stacklessBin = q.system.fs.joinPaths(q.dirs.baseDir, 'bin', 'python')
    stacklessBin = 'python'
    workflowengineBin = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','bin', 'main.py')
    #workflowengineProcess = '%s %s'%(stacklessBin, workflowengineBin)

    #pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine.pid')
    #stdoutFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.stdout')
    #stderrFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.stderr')

    #initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initSuccess')
    #initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initFailed')
    #config = i.config.workflowengine.getConfig('main')

    def start(self, appname):
        """
        Start workflow engine
        """

        pidFile = q.system.fs.joinPaths(q.dirs.pidDir,
            'workflowengine.%s.pid' % appname)
        stdoutFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
            'workflowengine.%s.stdout' % appname)
        stderrFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
            'workflowengine.%s.stderr' % appname)
        initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
            'workflowengine.%s.initSuccess' % appname)
        initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log',
            'workflowengine.%s.initFailed' % appname)

        if self.getStatus(appname) == q.enumerators.AppStatusType.RUNNING:
            print "The workflowengine is already running."
        else:
            config = self.getConfig(appname)
            port = int(config['port'])

            workflowengineProcess =  '%s %s --appname=%s --port=%d' % \
                (self.stacklessBin, self.workflowengineBin, appname, port)

            if q.system.process.getProcessByPort(port) <> None:
                print "Cannot start the workflowengine: another process is holding port %i: %s." % ( port, str(q.system.process.getProcessByPort(port)))
            else:
                if q.system.process.checkProcess(workflowengineProcess) == 0:
                    print "Cannot start the workflowengine: an other instance of the workflowengine is running."
                else:
                    print "Starting the workflowengine."
                    for file in [pidFile, stdoutFile, stderrFile, initSuccessFile, initFailedFile]:
                        if q.system.fs.exists(file): q.system.fs.remove(file)
                    pid = q.system.process.runDaemon(workflowengineProcess, stdout=stdoutFile,  stderr=stderrFile)
                    q.system.fs.writeFile(pidFile, str(pid))

                    print " Waiting for initialization"
                    while not (q.system.fs.exists(initSuccessFile) or q.system.fs.exists(initFailedFile)) and q.system.process.checkProcess(workflowengineProcess) == 0:
                        time.sleep(0.5)

                    if q.system.fs.exists(initSuccessFile):
                        print "Workflowengine started"
                    else:
                        print "INITIALIZATION FAILED, WORKFLOWENGINE NOT STARTED !"
                        print "  " + q.system.fs.fileGetContents(stderrFile).replace("\n", "\n  ")

    def stop(self, appname):
        """
        Stop workflow engine
        """
        if self.getStatus(appname) == q.enumerators.AppStatusType.RUNNING:
            print "Stopping the workflowengine."
            q.system.process.kill(int(self._getPid(appname)), sig=signal.SIGTERM)

            i = 10
            while self.ping(appname) or q.system.process.isPidAlive(int(self._getPid(appname))):
                if i > 0:
                    print "   Still running, waiting %i seconds..." % i
                    i -= 1
                    time.sleep(1)
                else:
                    print "Could not stop the workflowengine. Next step: kill it with q.manage.workflowengine.kill()."
                    return

            print "Stopped the workflowengine."
        else:
            print "The workflowengine is not running."

    def restart(self, appname):
        """
        Restart workflow engine
        """
        self.stop(appname)
        self.start(appname)

    def kill(self, appname):
        """
        Stop workflow engine
        """
        if self.getStatus(appname) == q.enumerators.AppStatusType.RUNNING:
            print "Killing the workflowengine."
            q.system.process.kill(int(self._getPid(appname)))
        else:
            print "The workflowengine is not running."

    def _getPid(self, appname):
        """
        Retrieve the pid from given file
        """
        pid = None

        pidFile = q.system.fs.joinPaths(q.dirs.pidDir,
            'workflowengine.%s.pid' % appname)

        if q.system.fs.exists(pidFile):
            pid = q.system.fs.fileGetContents(pidFile)
        return pid

    def getStatus(self, appname):
        """
        Retrieve the pid from given file
        """
        pid = self._getPid(appname)
        if pid \
            and q.system.process.isPidAlive(int(pid)) \
            and self.ping(appname):
            return q.enumerators.AppStatusType.RUNNING
        return q.enumerators.AppStatusType.HALTED


    def ping(self, appname):
        """
        Checks if workflowengine is still responsive
        """

        ping = yaml.dump('ping') + "\n---\n"
        msg = ''
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        config = self.getConfig(appname)
        port = int(config['port'])

        try:
            sock.connect(('localhost', port))
            sock.sendall(ping)
            sock.settimeout(5)
            msg = sock.recv(30)
        except:
            return False
        sock.close()
        return 'pong' in msg
    
    def getConfig(self, appname):
        config_path = q.system.fs.joinPaths(q.dirs.pyAppsDir, appname, 'cfg', 'wfe.cfg')
        
        if not q.system.fs.exists(config_path):
            raise ValueError('No WFE configuration found for application %s' % appname)
        
        f = q.tools.inifile.open(config_path)
        config = f.getFileAsDict()
        
        if not config.has_key('main'):
            raise ValueError('No valid WFE configuration found for application %s' % appname)

        return config['main']
