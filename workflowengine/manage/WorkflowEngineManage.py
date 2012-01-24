from pylabs import q
import signal, time

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

    def start(self, appname, debug=False):
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
            return True
        else:
            config = self.getConfig(appname)
            port = int(config['port'])

            workflowengineProcess =  '%s %s --appname=%s --port=%d' % \
                (self.stacklessBin, self.workflowengineBin, appname, port)

            if q.system.process.getProcessByPort(port) <> None:
                print "Cannot start the workflowengine: another process is holding port %i: %s." % ( port, str(q.system.process.getProcessByPort(port)))
                return False
            else:
                if q.system.process.checkProcess(workflowengineProcess) == 0:
                    print "Cannot start the workflowengine: an other instance of the workflowengine is running."
                    return False
                else:
                    print "Starting the workflowengine."
                    for file in [pidFile, stdoutFile, stderrFile, initSuccessFile, initFailedFile]:
                        if q.system.fs.exists(file): q.system.fs.remove(file)
                    if not debug:
                        pid = q.system.process.runDaemon(workflowengineProcess, stdout=stdoutFile,  stderr=stderrFile)
                    else:
                        q.system.process.execute("screen -dmS wfe.%s %s" %(appname, workflowengineProcess))
                        pids = q.system.process.getProcessPid(workflowengineProcess)
                        if pids:
                            pid = max(pids)
                        else:
                            raise RuntimeError("Failed to start Workfflowengine %s" % appname)
                    q.system.fs.writeFile(pidFile, str(pid))

                    print " Waiting for initialization"
                    while not (q.system.fs.exists(initSuccessFile) or q.system.fs.exists(initFailedFile)) and q.system.process.checkProcess(workflowengineProcess) == 0:
                        time.sleep(0.5)

                    if q.system.fs.exists(initSuccessFile):
                        print "Workflowengine started"
                        return True
                    else:
                        print "INITIALIZATION FAILED, WORKFLOWENGINE NOT STARTED !"
                        print "  " + q.system.fs.fileGetContents(stderrFile).replace("\n", "\n  ")
                        return False

    def stop(self, appname):
        """
        Stop workflow engine
        """
        if self.getStatus(appname) == q.enumerators.AppStatusType.RUNNING:
            print "Stopping the workflowengine."
            q.system.process.kill(int(self._getPid(appname)), sig=signal.SIGTERM)

            i = 10
            while q.system.process.isPidAlive(int(self._getPid(appname))):
                if i > 0:
                    print "   Still running, waiting %i seconds..." % i
                    i -= 1
                    time.sleep(1)
                else:
                    print "Could not stop the workflowengine. Next step: kill it with q.manage.workflowengine.kill()."
                    return False

            print "Stopped the workflowengine."
            return True
        else:
            print "The workflowengine is not running."
            return True

    def restart(self, appname, debug=False):
        """
        Restart workflow engine
        """
        self.stop(appname)
        self.start(appname, debug)

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
        if pid and q.system.process.isPidAlive(int(pid)):
            return q.enumerators.AppStatusType.RUNNING
        return q.enumerators.AppStatusType.HALTED


    def getConfig(self, appname):
        config_path = q.system.fs.joinPaths(q.dirs.pyAppsDir, appname, 'cfg', 'wfe.cfg')
        
        if not q.system.fs.exists(config_path):
            raise ValueError('No WFE configuration found for application %s' % appname)
        
        f = q.tools.inifile.open(config_path)
        config = f.getFileAsDict()
        
        if not config.has_key('main'):
            raise ValueError('No valid WFE configuration found for application %s' % appname)

        return config['main']
