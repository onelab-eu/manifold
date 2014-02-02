#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldXMLRPCClientXMLRPCLIB 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from manifold.core.announce         import Announce
from manifold.util.options          import Options
from manifold.util.type             import accepts, returns
from ..clients.client               import ManifoldClient

class ManifoldXMLRPCClientXMLRPCLIB(ManifoldClient):
    # on ne sait pas si c'est secure ou non

    def __init__(self):
        """
        Construcor.
        """
        self.auth = None
        super(ManifoldXMLRPCClientXMLRPCLIB, self).__init__()

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

    def make_router(self):
        """
        Returns an instance behaving like a Router.
        """
        import xmlrpclib
        url = Options().xmlrpc_url
        router = xmlrpclib.ServerProxy(url, allow_none=True)
        return router

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation to pass to the QUERY Packet
            sent to the Router.
        """
        return Annotation({"authentication" : self.auth}) 
 
    # def whoami() should be defined
    # def welcome_message() should be defined
