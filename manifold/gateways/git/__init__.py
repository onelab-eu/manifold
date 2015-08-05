#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MyPLC / PLE
#   http://www.planet-lab.eu
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC 

import socket
from types                              import StringTypes
from manifold.core.announce             import Announces, announces_from_docstring
from manifold.core.field_names          import FieldNames
from manifold.gateways                  import Gateway
from manifold.gateways.object           import ManifoldCollection
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 
from manifold.util.predicate            import eq, included

from subprocess import Popen, PIPE

class IPCollection(ManifoldCollection):
    """
    class git {
        const string url;
        const string log;

        CAPABILITY(join);
        KEY(url);
    };
    """

    def get(self, packet):
        destination = packet.get_destination()

        obj = destination.get_object()
        value_list = destination.get_filter().get_field_values("url")

        # We don't really ask something sometimes...
        if destination.get_field_names() == FieldNames(["url"]):
            records = [{"url": value} for value in value_list]
            self.get_gateway().records(records, packet)
            return

        url = value_list[0]

        print "PROCESSING URL", url
        from subprocess import call
        p1 = Popen(["dmesg"], stdout=PIPE)
        p2 = Popen(["grep", "hda"], stdin=p1.stdout, stdout=PIPE)
        p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        output = p2.communicate()[0]
        records = [{'url': url, 'log': output}]

        self.get_gateway().records(records, packet)

class GitGateway(Gateway):

    __gateway_name__ = "git"

    def __init__(self, router = None, platform_name = None, **platform_config):
        Gateway.__init__(self, router, platform_name, **platform_config)

        self.register_collection(GitLogCollection())
        #self.register_collection(HostnameCollection())
