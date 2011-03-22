import os
import sys

def _getParam(name):
    prefix = '--%s=' % name

    for item in sys.argv:
        if item.startswith(prefix):
            name = item[len(prefix):]

            if not name:
                raise ValueError('Invalid %s command arg' % name)

            return name

    raise RuntimeError('No %s command arg found' % name)

def getAppName():
    name = os.environ.get('TWISTED_NAME', None)
    if not name: return _getParam('appname')
    return name.split('.')[-1]

getPort = lambda: int(_getParam('port'))
