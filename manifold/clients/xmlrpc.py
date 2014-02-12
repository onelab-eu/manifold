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
from manifold.core.annotation       import Annotation
from manifold.core.result_value     import ResultValue
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

    @returns(ResultValue)
    def forward(self, query, annotation = None):
        """
        Forward a Query toward a Manifold XMLRPC server
        Args:
            query: A Query instance.
            annotation: An Annotation instance or None.
        Returns:
            The corresponding ResultValue.
        """
        if not annotation:
            annotation = Annotation() 
        annotation.update(self.annotation)
 
        Log.debug("Sending (q = %s, a = %s) to %s" % (query.to_dict(), annotation.to_dict(), self.router))
        result_value_dict = self.router.forward(query.to_dict(), annotation.to_dict())
        result_value = ResultValue(result_value_dict)
        if result_value.is_success():
            from manifold.core.record import Records
            result_value["value"] = Records(result_value["value"])
        return result_value
