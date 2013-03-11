#!/usr/bin/env python
# -*- coding:utf-8 */

from twisted.web import xmlrpc, server
from twisted.internet import reactor
from tophat.core.router import THLocalRouter
from manifold.core.query import Query 

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
        # Need parameter validation
        auth = args[0]
        s = router.get_session(auth)
        return s

    def xmlrpc_GetPersons(self, *args):
        """
        """
        user = router.authenticate(args[0])
        return [{'email': user.email, 'first_name': user.email, 'last_name': '', 'user_hrn': 'TODO'}]


    def xmlrpc_AddCredential(self, *args):
        """
        """
        # The first argument should be an authentication token
        auth, credential, platform = args
        user = router.authenticate(auth)
        # The second argument is the credential to add
        router.add_credential(credential, platform, user)
        return 1

    def xmlrpc_GetCredentials(self, *args):
        user = router.authenticate(args[0])
        platform = args[1]
        return router.get_credentials(platform, user);

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

    def xmlrpc_Get(self, *args):
        auth, method, filters, params, fields = args
        return self.xmlrpc_forward(auth, 'get', method, filters, params, fields)

    def xmlrpc_Create(self, *args):
        auth, method, params = args
        return self.xmlrpc_forward(auth, 'create', method, [], params, [])

def main():
    try:
        reactor.callFromThread(lambda: reactor.listenTCP(7080, server.Site(TopHatAPI(allowNone=True))))
    except Exception, e:
        print "E/", e
    print "XMLRPC server Listening..."

if __name__ == '__main__':
    main()
