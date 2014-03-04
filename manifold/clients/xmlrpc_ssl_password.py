#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Client using SSL password authentification and XMLRPC 
# to communicate with a Manifold Router
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes
from twisted.internet               import defer, ssl

from manifold.core.annotation       import Annotation
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.xmlrpc_proxy     import XMLRPCProxy
from manifold.util.type             import accepts, returns 
from ..clients.xmlrpc               import ManifoldXMLRPCClient

class ManifoldXMLRPCClientSSLPassword(ManifoldXMLRPCClient):
    
    def __init__(self, url, username = None, password = None):
        """
        Constructor:
            username: A String containing the user name to connect the XML server.
            password: A String containing the corresponding password.
        """
        super(ManifoldXMLRPCClientSSLPassword, self).__init__(url)
        self.username   = username
        self.password   = password
        self.annotation = self.get_annotation()
        self.router     = XMLRPCProxy(self.url, allowNone=True, useDateTime=False)

        self.router.setSSLClientContext(ssl.ClientContextFactory())

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation added into the QUERY Packet
            sent to the Router.
        """
        if self.username:
            annotation = Annotation({
                "authentication" : {
                    "AuthMethod" : "password",
                    "Username"   : self.username,
                    "AuthString" : self.password
                }
            })
        else:
            annotation = Annotation({
                "authentication" : {
                    "AuthMethod" : "anonymous"
                }
            }) 

        return annotation 
 
    @returns(StringTypes)
    def welcome_message(self):
        """
        Returns:
            A welcome message.
        """
        return "Shell using XMLRPC account '%s' (password) on %s" % (self.username, self.url)

    @defer.inlineCallbacks
    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        raise NotImplementedError
