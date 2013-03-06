#OBSOLETE|from tophat.core.processingnode import ProcessingNode
#OBSOLETE|from tophat.gateways.xmlrpc import XMLRPC
#OBSOLETE|from tophat.gateways.sfa import SFA
#OBSOLETE|
#OBSOLETE|class Gateway(ProcessingNode):
#OBSOLETE|    @staticmethod
#OBSOLETE|    def factory(api, query, **kwargs):
#OBSOLETE|        if not 'type' in kwargs:
#OBSOLETE|            raise Exception, "No type specified in gateway for platform '%(platform)s'" % kwargs
#OBSOLETE|        if kwargs['type'] == 'xmlrpc':
#OBSOLETE|            return XMLRPCGateway(api, query, **kwargs)
#OBSOLETE|        elif kwargs['type'] == 'sfa':
#OBSOLETE|            return SFAGateway(api, query, **kwargs)
#OBSOLETE|        else:
#OBSOLETE|            raise Exception, "Unknown type '%s' in gateway for platform '%s'" % (kwargs['type'], kwargs['platform'])
