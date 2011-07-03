__author__ = 'incubaid'
__priority__= 3

def main(q, i, p, params, tags):
    from pymodel.serializers import ThriftSerializer
    import base64
    job  = p.api.model.core.job.get(params['rootobjectguid'])
    params['result'] = base64.encodestring(ThriftSerializer.serialize(job))


def match(q, i, params, tags):
    return True


