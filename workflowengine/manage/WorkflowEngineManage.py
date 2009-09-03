from pymonkey import q
import signal

class WorkflowEngineManage:

    def start(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Start workflow engine
        @param pidFile: file path to save the pid
        """
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
        pid = self._getPid(pidFile)
        if self.getStatus(pid=pid) == q.enumerators.AppStatusType.RUNNING:
            q.system.process.kill(int(pid), sig=signal.SIGTERM)

    def kill(self, pidFile=q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Stop workflow engine
        @param pidFile: file path to retrieve the pid
        """
        pid = self._getPid(pidFile)
        if self.getStatus(pid=pid) == q.enumerators.AppStatusType.RUNNING:
            q.system.process.kill(int(pid))

    def _getPid(self, pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid')):
        """
        Retrieve the pid from given file
        @param pidFile: file path to retrieve the pid
        """
        pid = None
        if q.system.fs.exists(pidFile):
            pid = q.system.fs.fileGetContents(pidFile)

        return pid

    def getStatus(self,  pidFile = q.system.fs.joinPaths(q.dirs.pidDir, 'workflowengine', 'workflowengine.pid'), pid=None):
        """
        Retrieve the pid from given file
        @param pidFile: file path to retrieve the pid
        @param pid: pid to check if running
        """
        if not pid:
            pid = self._getPid(pidFile)
        if pid and q.system.process.isPidAlive(int(pid)):
            return q.enumerators.AppStatusType.RUNNING
        return q.enumerators.AppStatusType.HALTED



