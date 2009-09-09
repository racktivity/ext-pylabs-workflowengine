from pymonkey import q, i
import signal, time

class WorkflowEngineManage:
    stacklessBin = q.system.fs.joinPaths(q.dirs.baseDir, 'bin', 'stackless')
    workflowengineBin = q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','bin', 'main.py') 
    workflowengineProcess = '%s %s'%(stacklessBin, workflowengineBin)
    
    pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine.pid')
    stdoutFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.stdout')
    stderrFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.stderr')
    
    initSuccessFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initSuccess')
    initFailedFile = q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflowengine.initFailed')

    def start(self):
        """
        Start workflow engine
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "The workflowengine is already running."
        else:
            port = int(i.config.workflowengine.getConfig('main')['port'])
            if q.system.process.getProcessByPort(port) <> None:
                print "Cannot start the workflowengine: another process is holding port %i: %s." % ( port, str(q.system.process.getProcessByPort(i.config.workflowengine.getConfig('main')['port'])))
            else:
                if q.system.process.checkProcess(self.workflowengineProcess) == 0:
                    print "Cannot start the workflowengine: an other instance of the workflowengine is running."
                else:
                    print "Starting the workflowengine."
                    for file in [self.pidFile, self.stdoutFile, self.stderrFile, self.initSuccessFile, self.initFailedFile]:
                        if q.system.fs.exists(file): q.system.fs.remove(file)
                    pid = q.system.process.runDaemon(self.workflowengineProcess, stdout=self.stdoutFile,  stderr=self.stderrFile)
                    q.system.fs.writeFile(self.pidFile, str(pid))
                    
                    print " Waiting for initialization"
                    while not (q.system.fs.exists(self.initSuccessFile) or q.system.fs.exists(self.initFailedFile)) and q.system.process.checkProcess(self.workflowengineProcess) == 0:
                        time.sleep(0.5)
                    
                    if q.system.fs.exists(self.initSuccessFile):
                        print "Workflowengine started"
                    else:
                        print "INITIALIZATION FAILED, WORKFLOWENGINE NOT STARTED !"
                        print "  " + q.system.fs.fileGetContents(self.stderrFile).replace("\n", "\n  ")

    def stop(self):
        """
        Stop workflow engine
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "Stopping the workflowengine."
            q.system.process.kill(int(self._getPid()), sig=signal.SIGTERM)
            
            i = 10
            while self.getStatus() == q.enumerators.AppStatusType.RUNNING:
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

    def restart(self):
        """
        Restart workflow engine
        """
        self.stop()
        self.start()

    def kill(self):
        """
        Stop workflow engine
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "Killing the workflowengine."
            q.system.process.kill(int(self._getPid()))
        else:
            print "The workflowengine is not running."

    def _getPid(self):
        """
        Retrieve the pid from given file
        """
        pid = None
        if q.system.fs.exists(self.pidFile):
            pid = q.system.fs.fileGetContents(self.pidFile)
        return pid

    def getStatus(self):
        """
        Retrieve the pid from given file
        """
        pid = self._getPid()
        if pid and q.system.process.isPidAlive(int(pid)):
            return q.enumerators.AppStatusType.RUNNING
        return q.enumerators.AppStatusType.HALTED

