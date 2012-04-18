__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    sql= '''SELECT DISTINCT JOBLIST.guid,
                JOBLIST.parentjobguid,
                JOBLIST.actionname,
                JOBLIST.name,
                JOBLIST.description,
                JOBLIST.starttime,
                JOBLIST.endtime,
                JOBLIST.jobstatus,
                JOBLIST.joborder
                FROM ONLY core_job.core_view_job_list JOBLIST
                where JOBLIST.guid in (
                                                WITH RECURSIVE childjobs AS
                                                (
                                                    -- non-recursive term
                                                    SELECT core_job.core_view_job_list.guid
                                                    FROM core_job.core_view_job_list
                                                    WHERE core_job.core_view_job_list.parentjobguid = %(rootobjectguid)s

                                                    UNION ALL

                                                    -- recursive term
                                                    SELECT jl.guid
                                                    FROM core_job.core_view_job_list AS jl
                                                    JOIN
                                                        childjobs AS cj
                                                        ON (jl.parentjobguid = cj.guid)
                                                )
                                                SELECT guid from childjobs
                                      )
                or JOBLIST.guid = %(rootobjectguid)s
                ORDER BY JOBLIST.joborder;'''

    jobs = p.api.model.core.job.query(sql, {'rootobjectguid': params['rootobjectguid']})
    params['result'] = jobs

def match(q, i, params, tags):
    return True
