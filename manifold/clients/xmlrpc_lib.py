#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ManifoldXMLRPCClientXMLRPCLIB 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

from types                          import StringTypes

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
        import xmlrpclib
        url = Options().xmlrpc_url
        self.router = xmlrpclib.ServerProxy(url, allow_none=True)
        self.auth = None

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
 
    @returns(StringTypes)
    def welcome_message(self):
        """
        Method that should be overloaded and used to log
        information while running the Query (level: INFO).
        Returns:
            A welcome message
        """
        raise NotImplementedError

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        raise NotImplementedError

    def forward(self, query, annotation = None):
        """
        Send a Query to the XMLRPC server. 
        Args:
            query: A Query instance.
            annotation: The corresponding Annotation instance (if
                needed) or None.
        Results:
            The ResultValue resulting from this Query.
        """
        if not annotation:
            annotation = Annotation() 
        annotation = self.get_annotation()

        return ResultValue(self.router.forward(query.to_dict(), annotation.to_dict()))
