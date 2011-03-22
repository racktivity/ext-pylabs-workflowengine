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

getAppName = lambda: _getParam('appname')
getPort = lambda: int(_getParam('port'))
