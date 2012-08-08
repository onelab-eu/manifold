from tophat.core.processingnode import ProcessingNode
from tophat.gateways.xmlrpc import XMLRPC
from tophat.gateways.sfa import SFA

class Gateway(ProcessingNode):
    @staticmethod
    def factory(api, query, **kwargs):
        if not 'type' in kwargs:
            raise Exception, "No type specified in gateway for platform '%(platform)s'" % kwargs
        if kwargs['type'] == 'xmlrpc':
            return XMLRPCGateway(api, query, **kwargs)
        elif kwargs['type'] == 'sfa':
            return SFAGateway(api, query, **kwargs)
        else:
            raise Exception, "Unknown type '%s' in gateway for platform '%s'" % (kwargs['type'], kwargs['platform'])
