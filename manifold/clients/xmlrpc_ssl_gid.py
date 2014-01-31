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
from twisted.internet               import ssl

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
        self.url = url
        self.gid_subject = '(TODO: extract from cert_file)' # XXX 
        self.router = XMLRPCProxy(self.url, allowNone=True, useDateTime=False)
        #self.router.setSSLClientContext(CtxFactory(pkey_file, cert_file))

        self.annotation = Annotation({
            "authentication": {
                "AuthMethod": "gid"
            }
        })

        # This has to be tested to get rid of the previously defined CtxFactory class
        self.router.setSSLClientContext(ssl.DefaultOpenSSLContextFactory(pkey_file, cert_file))

    @returns(StringTypes)
    def welcome_message(self):
        return "Shell using XMLRPC account '%r' (GID) on %s" % (self.gid_subject, self.url)

