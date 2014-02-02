#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Client using SSL GID authentification and XMLRPC 
# to communicate with a Manifold Router
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

from types                          import StringTypes
from twisted.internet               import defer, ssl

from manifold.core.annotation       import Annotation
from manifold.util.xmlrpc_proxy     import XMLRPCProxy
from manifold.util.type             import accepts, returns 
from ..clients.xmlrpc               import ManifoldXMLRPCClient

class ManifoldXMLRPCClientSSLGID(ManifoldXMLRPCClient):
    
    ## We need this to define the private key and certificate (GID) to use
    #class CtxFactory(ssl.ClientContextFactory):
    #    def getContext(self):
    #        self.method = SSL.SSLv23_METHOD
    #        ctx = ssl.ClientContextFactory.getContext(self)
    #        ctx.use_certificate_chain_file(self.cert_file)
    #        ctx.use_privatekey_file(self.pkey_file)
    #        return ctx

    def __init__(self, url, pkey_file, cert_file):
        """
        Constructor.
        Args:
            url: A String containing the URI of the XMLRPC server queried
                by this client (ex "http://localhost:7080").
            pkey_file: A String containing the absolute path of the private
                key used to connect to the XMLRPC server.
            cert_file: A String containing the absolute path of the private
                key used to connect to the XMLRPC server.
        """
        self.pkey_file = pkey_file
        self.cert_file = cert_file
        super(ManifoldXMLRPCClientSSLGID, self).__init__(url)

        self.gid_subject = "(TODO: extract from cert_file = %s)" % cert_file # XXX 

    #--------------------------------------------------------------
    # Overloaded methods 
    #--------------------------------------------------------------

    #@returns(XMLRPCProxy)
    def make_router(self):
        """
        Returns:
            A XMLRPCProxy behaving like a Router.
        """
        router = XMLRPCProxy(self.url, allowNone = True, useDateTime = False)
        #self.router.setSSLClientContext(CtxFactory(self.pkey_file, self.cert_file))
        # This has to be tested to get rid of the previously defined CtxFactory class
        router.setSSLClientContext(ssl.DefaultOpenSSLContextFactory(self.pkey_file, self.cert_file))
        return router

    @returns(Annotation)
    def get_annotation(self):
        """
        Returns:
            An additionnal Annotation added into the QUERY Packet
            sent to the Router.
        """
        return Annotation({
            "authentication": {
                "AuthMethod": "gid"
            }
        })

    @returns(StringTypes)
    def welcome_message(self):
        """
        Returns:
            A welcome message.
        """
        return "Shell using XMLRPC account '%r' (GID) on %s" % (self.gid_subject, self.url)

    @defer.inlineCallbacks
    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently
            running the Manifold Client.
        """
        raise NotImplementedError
