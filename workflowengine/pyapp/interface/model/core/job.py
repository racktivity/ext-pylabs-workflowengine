from pylabs.baseclasses.BaseEnumeration import BaseEnumeration
import pymodel as model

# @doc Job status enumeration
class jobstatus(BaseEnumeration):
    @classmethod
    def _initItems(cls):
        cls.registerItem('RUNNING')
        cls.registerItem('DONE')
        cls.registerItem('WAITING')
        cls.registerItem('ERROR')
        cls.finishItemRegistration()

# @doc class that provides the properties of a job
class job(model.RootObjectModel):

    #@doc name of the object
    name = model.String(thrift_id=1)

    #@doc description of the object
    description = model.String(thrift_id=2)

    #@doc actionname e.g. machine.start, can be "" if job not result of an action
    actionName = model.String(thrift_id=3)

    #@doc error message which is userfriendly for user to see
    userErrormsg = model.String(thrift_id=4)

    #@doc error message only for internal usage
    internalErrormsg = model.String(thrift_id=5)

    #@doc max duration in seconds
    maxduration = model.Integer(thrift_id=6)

    #@doc guid of the parent job
    parentjobguid = model.GUID(thrift_id=7)

    #@doc status of the job
    jobstatus = model.Enumeration(jobstatus,thrift_id=8)

    #@doc the date and time when the job is started
    starttime = model.DateTime(thrift_id=9)

    #@doc the date and time when the job has finished
    endtime = model.DateTime(thrift_id=10)

    #@doc guid of the cloud user who has initiated the job
    clouduserguid = model.GUID(thrift_id=11)

    #@doc Guid of the agent that executed this job
    agentguid = model.String(thrift_id=12)

    #@doc incremental nr for this job inside the parent job
    order = model.Integer(thrift_id=13)

    #@doc log text, is the complete log of a jobstep, logs are also on logserver opgeslaan, is in format \$epoch|\$loglevelIsInt|\$source|\$the logtext, can become quite big
    log = model.String(thrift_id=14)

    #@doc type of the rootobject that started the job
    rootobjecttype = model.String(thrift_id=15)

    #@doc Guid of the rootobject that started the job
    rootobjectguid = model.GUID(thrift_id=16)

    #@doc params sent to job
    params = model.String(thrift_id=17)
    
    #@doc system
    system = model.Boolean(thrift_id=18)

    #@doc series of tags format
    tags = model.String(thrift_id=19)
