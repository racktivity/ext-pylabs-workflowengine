__author__ = 'incubaid'
__priority__= 3

import osis
from pymodel.serializers import YamlSerializer

def main(q, i, p, params, tags):
    job = p.api.model.core.job.get(params['yamljobguid'])
    if not job: raise ValueError('Job with guid % not found.' % params['jobguid'])
    params['result'] = YamlSerializer.serialize(job)

def match(q, i, params, tags):
    return True

