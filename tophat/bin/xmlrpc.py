#!/usr/bin/env python
# -*- coding:utf-8 */

from twisted.web import xmlrpc, server
from twisted.internet import reactor
from tophat.core.router import THLocalRouter
from tophat.core.query import Query 

router = THLocalRouter()
router.__enter__()

class TopHatAPI(xmlrpc.XMLRPC):
    """
    An example object to be published.
    """

    def xmlrpc_AuthCheck(self, *args):
        """
        """
        return 1

    def xmlrpc_GetSession(self, *args):
        """
        """
        return 1

    def xmlrpc_GetPersons(self, *args):
        """
        """
        return [{'email': 'demo', 'first_name': 'first', 'last_name': 'last', 'person_hrn': 'myslice.demo'}]

    def xmlrpc_AddCredential(self, *args):
        """
        """
        pass

    def xmlrpc_forward(self, *args):
        """
        """
        print "Handling query: ", args

        query = THQuery(*args)

        table = router.forward(query, deferred=True)

        return table

def main():
    try:
        reactor.callFromThread(lambda: reactor.listenTCP(7080, server.Site(TopHatAPI(allowNone=True))))
    except Exception, e:
        print "E/", e
    print "XMLRPC server Listening..."

if __name__ == '__main__':
    query1 = THQuery(action='get', fact_table='tophat:platform', filters=[], params=None, fields=['platform', 'platform_longname'])
    print router.forward(query1)
    main()
