from workflowengine.SharedMemory import open_shm, close_shm
import ast

def main():
    currentjob = open_shm("wfe-current-job")
    currentjob.seek(0)
    print("Currently running: " + currentjob.readline())
    close_shm("wfe-current-job", currentjob)

    jobs = open_shm("wfe-jobs")
    jobs.seek(0)
    jobs_string = ""
    line = jobs.readline()
    while line <> "---\n":
        jobs_string += line
        line = jobs.readline()

    close_shm("wfe-jobs", jobs)
    jobs_content =  ast.literal_eval(jobs_string)

    for jobguid, details in jobs_content.iteritems():
        print "Job " + str(jobguid)
        print "  actionName " + str(details['actionname'])
        print "  parentjobguid " + str(details['parentjobguid'])
        print "  jobstatus " + str(details['jobstatus'])
        print "  starttime " + str(details['starttime'])
        print "  agentguid " + str(details['agentguid'])
        print


if __name__ == '__main__':
    main()
