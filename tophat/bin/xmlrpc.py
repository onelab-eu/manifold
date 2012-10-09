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
        user = router.authenticate(args[0])
        print "I: args", args
        return [{'email': 'EMAIL', 'first_name': 'FIRSTNAME', 'last_name': 'LASTNAME', 'person_hrn': 'HRN_TODO'}]


    def xmlrpc_AddCredential(self, *args):
        """
        """
        # The first argument should be an authentication token
        email, credential, platform = args
        user = router.authenticate(email)
        # The second argument is the credential to add
        router.add_credential(credential, platform, user)
        return 1

    def xmlrpc_forward(self, *args):
        """
        """
        # The first argument should be an authentication token
        user = router.authenticate(args[0])
        args = list(args)
        args = args[1:]
        # The rest define the query
        query = Query(*args)

        table = router.forward(query, deferred=True, user=user)

        return table

def main():
    try:
        reactor.callFromThread(lambda: reactor.listenTCP(7080, server.Site(TopHatAPI(allowNone=True))))
    except Exception, e:
        print "E/", e
    print "XMLRPC server Listening..."

if __name__ == '__main__':
    main()
