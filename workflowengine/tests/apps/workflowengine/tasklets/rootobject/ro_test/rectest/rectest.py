__tags__ = 'ro_test', 'rectest'

import time

def main(q, i, params, tags):

    start = time.time()

    x = params['x']
    y = params['y']
    serialize = params['serialize']
    wait_in_sec = params['wait_in_sec']

    def rec(p):
        q.workflowengine.actionmanager.startRootobjectAction('ro_test', 'rectest', p)

    p = {'x': 0, 'y': 0, 'serialize': serialize, 'wait_in_sec': wait_in_sec}
    if serialize:
        for _ in xrange(0, (x-1)):
            rec(p)
    else:
        raise NotImplemented

    if y > 1:
        rec({'x': x, 'y': (y-1), 'serialize': serialize, 'wait_in_sec': wait_in_sec})

    stop = time.time()

    params['result'] = (stop - start)
