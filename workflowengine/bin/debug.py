from workflowengine.SharedMemory import open_shm, close_shm, write_shm
import yaml

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
	jobs_content = yaml.load(jobs_string)
	for jobguid in jobs_content:
		job = jobs_content[jobguid]
		print "Job " + str(jobguid)
		print "  actionName " + str(job[0])
		print "  parentjobguid " + str(job[1])
		print "  jobstatus " + str(job[2])
		print "  starttime " + str(job[3])
		print "  agentguid " + str(job[4])
		print

if __name__ == '__main__':
	main()
