#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldXMLRPCClient is the base class inherited by
# any Manifold client using XMLRPC
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from twisted.internet               import defer

from manifold.clients.client        import ManifoldClient
from manifold.util.log              import Log 
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

class ManifoldXMLRPCClient(ManifoldClient):
    def __init__(self, url):
        """
        Constructor
        Args:
            url: A String containing the URI of the XMLRPC server queried
                by this client (ex "http://localhost:7080").
        """
        super(ManifoldXMLRPCClient, self).__init__()
        self.url = url
        ReactorThread().start_reactor()

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

    def __del__(self):
        """
        Shutdown gracefully self.router 
        """
        ReactorThread().stop_reactor()

    @defer.inlineCallbacks
    @returns(dict)
    def whoami(self, query, annotation = None):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        Log.warning("ManifoldXMLRPCClient::whoami: Not yet implemented")
        #if not annotation:
        #    annotation = Annotation() 
        #annotation.update(self.annotation)
        #ret = yield self.router.AuthCheck(annotation)
        #defer.returnValue(ret)


