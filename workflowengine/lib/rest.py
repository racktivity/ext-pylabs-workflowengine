import json

from concurrence import dispatch
from concurrence.web import Application, Controller, web
from concurrence.web.filter import  JSONFilter

class JobView(object):
    '''Extracts relevant job info for lists'''

    def summarize(self, job):
        j = job.drp_object
        return {'guid': j.guid,
                'cloudusername': j.clouduserguid,
                'actionname': j.actionName,
                'description': j.description,
                'jobstatus': str(j.jobstatus),
                'starttime': str(j.starttime),
                'endtime': str(j.endtime),
                'parentjobguid': j.parentjobguid,
                'name': j.name,
                'joborder': j.order,
                'executor': j.agentguid,
               }

    def get_info(self, job):
        j = job.drp_object
        return {'guid': j.guid,
                'cloudusername': j.clouduserguid,
                'actionname': j.actionName,
                'description': j.description,
                'jobstatus': str(j.jobstatus),
                'starttime': str(j.starttime),
                'endtime': str(j.endtime),
                'parentjobguid': j.parentjobguid,
                'name': j.name,
                'joborder': j.order,
                'executor': j.agentguid,
                'log': j.log,
               }

    def __call__(self, jobs, summary=True):

        f = self.summarize if summary else self.get_info

        if getattr(jobs, '__iter__', False):
            j = map(f, jobs)
        else:
            j = f(jobs)

        return json.dumps(j)

class RestController(Controller):

    __filters__ = [JSONFilter()]

    def __init__(self, jobmgr):
        self.jobmgr = jobmgr
        self.view = JobView()

    @web.route('/job/status')
    def status(self):
        return json.dumps(self.jobmgr.status())

    @web.route('/job/running')
    def running(self):
        return self.view(self.jobmgr.list_running())

    @web.route('/job/root')
    def root(self):
        return self.view( self.jobmgr.list_root_jobs())

    @web.route('/job/tree')
    def tree(self):
        g = self.request.params.getone('jobguid')

        if not g:
            return self.view([])

        return self.view(sorted(self.jobmgr.list_rootjob_with_children(g), key=lambda j: j.drp_object.order))

class RestService(object):
    def __init__(self, jobmgr):
	self.app = Application()
        self.app.add_controller(RestController(jobmgr))
        self.app.configure()

    def start(self):
        self.app.serve(('0.0.0.0', 8080))
