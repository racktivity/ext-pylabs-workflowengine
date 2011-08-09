class job:
    """
    in job embedded there are all jobsteps + logging info
    """

    def create(self, jobguid="", executionparams=None):
        """
        Create a new job.

        @param jobguid:          guid of the job if available else empty string
        @type jobguid:           guid

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary

        @return:                 dictionary with backplaneguid as result and jobguid: {'result': guid, 'jobguid': guid}
        @rtype:                  dictionary

        @raise e:                In case an error occurred, exception is raised
        """

    def find(self, actionname="", agentguid="",rootobjectguid="",rootobjecttype="", fromTime="",toTime="",clouduserguid="",jobstatus='', jobguid="",executionparams=None):
        """        
        @execution_method = sync
        
        @param actionname:       actionname of the jobs to find
        @type actionname:        string

        @param rootobjectguid:   rootobjectguid of the jobs to find
        @type rootobjectguid:    guid

        @param rootobjecttype:   rootobjecttype of the jobs to find
        @type rootobjecttype:    guid

        @param fromTime:         starttime of the jobs to find (equal or greater than)
        @type fromTime:          datetime

        @param toTime:           endtime of the jobs to find (equal or less than)
        @type toTime:            datetime
        
        @param clouduserguid:    guid of the job user executing the job
        @type clouduserguid:     guid
        
        @param jobstatus:        status of the job
        @type jobstatus:         string
        
        @param jobguid:          guid of the job if available else empty string
        @type jobguid:           guid

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary
        
        @returns array of array [[...]]
        """

    def getLogInfo(self,rootobjectguid, jobguid='', MaxLogLevel=5, executionparams=None):
        """
        return log info as string
        @todo define format
        
        @execution_method = sync

        @param rootobjectguid:   guid of the job rootobject
        @type rootobjectguid:    guid

        @param jobguid:          guid of the job if available else empty string
        @type jobguid:           guid

        @param MaxLogLevel:      Specifies the highest log level
        @type MaxLogLevel:       integer

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary
        
        @return:                 job log info
        @rtype:                  string
        
        @todo:                   Will be implemented in phase2
        """
        raise NotImplementedError('Not implemented yet')

    def getObject(self, rootobjectguid, jobguid="",executionparams=None):
        """
        Gets the rootobject.

        @execution_method = sync
        
        @param rootobjectguid:   guid of the job rootobject
        @type rootobjectguid:    guid

        @param jobguid:          guid of the job if available else empty string
        @type jobguid:           guid

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary

        @return:                 rootobject
        @rtype:                  rootobject

        @warning:                Only usable using the python client.
        """

    def getYAML(self, yamljobguid, jobguid="", executionparams=None):
        """
        Gets a string representation in YAML format of the job rootobject.

        @execution_method = sync
        
        @param yamljobguid:       guid of the job rootobject
        @type yamljobguid:        guid

        @param jobguid:           guid of the job if available else empty string
        @type jobguid:            guid

        @param executionparams:   dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:    dictionary

        @return:                  YAML representation of the job
        @rtype:                   string
        """

    def getXML(self, jobguid, executionparams=None):
        """
        Gets a string representation in XML format of the job rootobject.

        @execution_method = sync
        
        @param jobguid:           guid of the job if available else empty string
        @type jobguid:            guid

        @param executionparams:   dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:    dictionary

        @return:                  XML representation of the job
        @rtype:                   string

        @raise e:                 In case an error occurred, exception is raised

        @todo:                    Will be implemented in phase2
        """
        raise NotImplementedError('Not implemented yet.')

    def getXMLSchema(self, jobguid, executionparams=None):
        """
        Gets a string representation in XSD format of the job rootobject structure.

        @execution_method = sync
        
        @param jobguid:          guid of the job rootobject
        @type jobguid:           guid

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary

        @return:                 XSD representation of the job structure.
        @rtype:                  string

        @raise e:                In case an error occurred, exception is raised

        @todo:                   Will be implemented in phase2
        """
        raise NotImplementedError('Not implemented yet.')

    def getJobTree(self, rootobjectguid, jobguid="",executionparams=None):
        """
        Gets the full tree of the rootobject.

        @execution_method = sync
        
        @param rootobjectguid:   guid of the job rootobject
        @type rootobjectguid:    guid

        @param jobguid:          guid of the job if available else empty string
        @type jobguid:           guid

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary

        @return:                 jobtree
        @rtype:                  array of dict [{...}]
        """

    def findLatestJobs(self, maxrows=5, errorsonly=False, jobguid="", executionparams=None):
        """
        Returns the latest jobs.

        @execution_method = sync
        
        @param maxrows:          specifies the number of jobs to return
        @type maxrows:           int

        @param errorsonly:       When True, only the latest <maxrows> ERROR jobs will be returned, otherwise the latest <maxrows> ERROR/RUNNING jobs will be returned
        @type errorsonly:        boolean

        @param executionparams:  dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:   dictionary

        @return:                 jobtree
        @rtype:                  array of dict [{...}]
        """
        
    def delete(self, jobguids, jobguid="",executionparams=None):
        """
        Delete all specified jobs and their children.
        
        @security: administrator
        
        @execution_method = sync
        
        @param jobguids:                 List of jobguids to delete           
        @type jobguids:                  array
        
        @param jobguid:                  guid of the job if available else empty string
        @type jobguid:                   guid

        @param executionparams:          dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:           dictionary
        
        @return:                         dictionary with True as result and jobguid: {'result': True, 'jobguid': guid}
        @rtype:                          dictionary

        @raise e:                        In case an error occurred, exception is raised
        """
        
    def clear(self, jobguid="",executionparams=None):
        """
        Deletes all jobs.
        
        @execution_method = sync
        
        @security: administrator
        
        @param jobguid:                  guid of the job if available else empty string
        @type jobguid:                   guid

        @param executionparams:          dictionary of job specific params e.g. userErrormsg, maxduration ...
        @type executionparams:           dictionary
        
        @return:                         dictionary with True as result and jobguid: {'result': True, 'jobguid': guid}
        @rtype:                          dictionary

        @raise e:                        In case an error occurred, exception is raised
        """
