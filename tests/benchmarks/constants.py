class _dummy:
    pass

Constants = _dummy()

Constants.transports = _dummy()
Constants.transports.xmlrpc = 1
Constants.transports.rest = 1
Constants.transports.mail = 1

transports = [ i for i in dir(Constants.transports) if not i.startswith('__')]
