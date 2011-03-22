from pylabs import q
from socket import *
from workflowengine.SharedMemory import open_shm, close_shm, write_shm
from workflowengine.WFLJobManager import WFLJobManager
import yaml
import ast
import posix_ipc

class WFEDebug:
    @q.manage.applicationserver.expose
    def getJobInfo(self):
        jobinfo = []
        #jobinfo.extend(self._getJobDetail("wfe-current-job"))
        jobinfo.extend(self._getJobDetail("wfe-jobs"))
        return jobinfo

    @q.manage.applicationserver.expose
    def killJob(self, jobguid):
        s = socket(AF_INET, SOCK_STREAM)
        s.settimeout(5)
        s.connect(('localhost', 1234))
        s.send(yaml.dump({"action":"kill_job", "jobguid":jobguid}))
        s.send("---\n")

        buffer = ""
        while True:
            data = s.recv(1024)
            if '---' not in data:
                buffer += data
            else:
                buffer += data[:data.index('---')]
                try:
                    response = yaml.load(buffer)
                    s.close()
                    return response
                except yaml.parser.ParserError:
                    raise


    def _getJobDetail(self, sharedMemoryName):
        # if no  jobs -> no sharedmemory in use -> throws an exception. Return an empty list
        jobinfo = []
        try:
            jobs = open_shm(sharedMemoryName)
            print jobs
        except posix_ipc.ExistentialError:
            print 'Nothing'
            return jobinfo

        jobs.seek(0)
        jobs_string = ""
        line = jobs.readline()
        while line <> "---\n":
            jobs_string += line
            line = jobs.readline()
        close_shm(sharedMemoryName, jobs, False)
        jobinfo = ast.literal_eval(jobs_string)

        return jobinfo.values()
