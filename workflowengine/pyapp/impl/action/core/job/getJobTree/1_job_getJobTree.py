__author__ = 'incubaid'
__priority__= 3

from osis.store import OsisConnection

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
                FROM ONLY %(table)s JOBLIST
                where JOBLIST.guid in (
                                                WITH RECURSIVE childjobs AS
                                                (
                                                    -- non-recursive term
                                                    SELECT %(table)s.guid
                                                    FROM %(table)s
                                                    WHERE %(table)s.parentjobguid = '%(rootobjectguid)s'

                                                    UNION ALL

                                                    -- recursive term
                                                    SELECT jl.guid
                                                    FROM %(table)s AS jl
                                                    JOIN
                                                        childjobs AS cj
                                                        ON (jl.parentjobguid = cj.guid)
                                                )
                                                SELECT guid from childjobs
                                      )
                or JOBLIST.guid = '%(rootobjectguid)s'
                ORDER BY JOBLIST.joborder;'''

    table = OsisConnection.getTable(domain = 'core', objType = 'job')
    jobs = p.api.model.core.job.query(sql % {'rootobjectguid': params['rootobjectguid'],
                                             'table' : table})
    params['result'] = jobs

def match(q, i, params, tags):
    return True
