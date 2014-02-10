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
    #        ctx.use_certificate_chain_file(self.cert_filename)
    #        ctx.use_privatekey_file(self.pkey_filename)
    #        return ctx

    def __init__(self, url, pkey_filename, cert_filename):
        """
        Constructor.
        Args:
            url: A String containing the URI of the XMLRPC server queried
                by this client (ex "http://localhost:7080").
            pkey_filename: A String containing the absolute path of the private
                key used to connect to the XMLRPC server.
            cert_filename: A String containing the absolute path of the private
                key used to connect to the XMLRPC server.
        """
        super(ManifoldXMLRPCClientSSLGID, self).__init__(url)
        self.pkey_filename = pkey_filename
        self.cert_filename = cert_filename
        self.gid_subject   = "(TODO: extract from cert_filename = %s)" % cert_filename # XXX 
        self.router        = XMLRPCProxy(self.url, allowNone = True, useDateTime = False)
        self.annotation    = self.get_annotation()

        # This has to be tested to get rid of the previously defined CtxFactory class
        #self.router.setSSLClientContext(CtxFactory(self.pkey_filename, self.cert_filename))
        self.router.setSSLClientContext(ssl.DefaultOpenSSLContextFactory(self.pkey_filename, self.cert_filename))

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
