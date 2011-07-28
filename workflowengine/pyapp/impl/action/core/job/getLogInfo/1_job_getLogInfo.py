__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S '

    def formatJobLog(joblog):
        from datetime import datetime
        import re
        result = ''
        if joblog:
            logs = list()
            joblog = joblog.replace('startHtmlBlock', '\nstartHtmlBlock')
            joblog = joblog.replace('endHtmlBlock', 'endHtmlBlock\n')
            for log in joblog.splitlines():
                match = re.search(',password=(?P<pwd>.*)]', log)
                if match:
                    log = log.replace(match.group('pwd'), '***')
                match = re.search(':passwd=(?P<pwd>.*) on', log)
                if match:
                    log = log.replace(match.group('pwd'), '***')
                match = re.search('://(?P<login>.+):(?P<pwd>.+)@', log)
                if match:
                    log = log.replace(match.group('pwd'), '***')
                match = re.search("'smbPassword': '(?P<pwd>.*)', 'smb"  , log)
                if match:
                    log = log.replace(match.group('pwd'), '***')
                if not log.startswith('startHtmlBlock'):
                    log = log.replace('<', '&lt;')
                    log = log.replace('>', '&gt;')
                    if '|' in log:
                        logParts = log.split('|')
                        dt = datetime.fromtimestamp(float(logParts[0]))
                        logParts[0] = dt.strftime(TIMESTAMP_FORMAT)
                        logContent = logParts[-1].split('//',1)[-1]
                        logParts = [logParts[0], logContent]
                        logs.append(' | '.join(logParts))
                    else:
                        logs.append(log)
                else:
                    log = log.replace('startHtmlBlock', '')
                    log = log.replace('endHtmlBlock', '')
                    logs.append(log)
            result = '\n'.join(logs)
        return result

    jobGuid = params.get('rootobjectguid', None)
    if not jobGuid:
        params['result'] = ""
        return
    job = p.api.action.core.job.getObject(jobGuid)

    params['result'] = formatJobLog(job.log)

def match(q, i, params, tags):
    return True

