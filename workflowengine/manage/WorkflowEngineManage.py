from pymonkey import q
import signal, time

class WorkflowEngineManage:

    def start(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Start workflow engine
        @param pidFile: file path to save the pid
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "The workflowengine is already running."
        else:
            print "Starting the workflowengine."
            pid = q.system.process.runDaemon('%s %s'%(q.system.fs.joinPaths(q.dirs.baseDir, 'bin', 'stackless'), q.system.fs.joinPaths(q.dirs.appDir,'workflowengine','bin', 'main.py')), stdout=q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflow.stdout'),  stderr=q.system.fs.joinPaths(q.dirs.varDir, 'log', 'workflow.stderr'))
            pidDir = q.system.fs.getDirName(pidFile)
            if not q.system.fs.exists(pidDir):
                q.system.fs.createDir(pidDir)
            q.system.fs.writeFile(pidFile, str(pid))

    def stop(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Stop workflow engine
        @param pidFile: file path to retrieve the pid
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "Stopping the workflowengine."
            pid = self._getPid(pidFile)
            q.system.process.kill(int(pid), sig=signal.SIGTERM)
            
            i = 10
            while self.getStatus() == q.enumerators.AppStatusType.RUNNING:
                if i > 0:
                    print "   Still running, waiting %i seconds..." % i
                    i -= 1
                    time.sleep(1)
                else:
                    print "Could not stop the workflowengine. Next step: kill it !"
                    return
                
            print "Stopped the workflowengine."
        else:
            print "The workflowengine is not running."

    def restart(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Restart workflow engine
        @param pidFile: file path to retrieve the pid
        """
        self.stop(pidFile)
        self.start(pidFile)

    def kill(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Stop workflow engine
        @param pidFile: file path to retrieve the pid
        """
        if self.getStatus() == q.enumerators.AppStatusType.RUNNING:
            print "Killing the workflowengine."
            pid = self._getPid(pidFile)
            q.system.process.kill(int(pid))
        else:
            print "The workflowengine is not running."

    def _getPid(self, pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Retrieve the pid from given file
        @param pidFile: file path to retrieve the pid
        """
        pid = None
        if q.system.fs.exists(pidFile):
            pid = q.system.fs.fileGetContents(pidFile)

        return pid

    def getStatus(self,  pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Retrieve the pid from given file
        @param pidFile: file path to retrieve the pid
        @param pid: pid to check if running
        """
        pid = self._getPid(pidFile)
        if pid and q.system.process.isPidAlive(int(pid)):
            return q.enumerators.AppStatusType.RUNNING
        return q.enumerators.AppStatusType.HALTED

