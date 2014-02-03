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

    def init_router(self):
        """
        Initialize self.router.
        """
        import xmlrpclib
        url = Options().xmlrpc_url
        self.router = xmlrpclib.ServerProxy(url, allow_none=True)

    def __init__(self):
        """
        Construcor.
        """
        self.auth = None
        super(ManifoldLocalClient, self).__init__()

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation to pass to the QUERY Packet
            sent to the Router.
        """
        return Annotation({"authentication" : self.auth}) 
 
