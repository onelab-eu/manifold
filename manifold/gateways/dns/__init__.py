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
from manifold.core.announce             import Announce, announces_from_docstring
from manifold.core.fields               import Fields
from manifold.gateways                  import Gateway
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 
from manifold.util.predicate            import eq, included

class DNSGateway(Gateway):

    __gateway_name__ = "dns"

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this gateway.
        """
        return "<DNSGateway>"

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def get_value_list(self, token, query):
        assert token in ['ip', 'hostname']

        value_list = []
        where = query.get_filter()

        predicate_list = []
        predicate_list.extend(where.get(token))
        predicate_list.extend(where.get((token,)))

        get_value = lambda value: value[0] if isinstance(value, tuple) else value

        for predicate in predicate_list:
            key, op, value = predicate.get_tuple()
            if op is eq:
                value_list.append(get_value(value))
            elif op is included:
                value_list.extend([get_value(v) for v in value])
            else:
                raise RuntimeError(
                    """
                    The WHERE clause (%s) must have exaclty one Predicate '==' involving 'ip' field.
                    Matching Predicate(s): {%s}
                    """ % (
                        where,
                        ', '.join(predicates)
                    )
                )
        return value_list

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()

        obj = query.get_object()
        value_list = query.get_filter().get_field_values(obj)

        # We don't really ask something sometimes...
        if query.get_fields() == Fields([obj]):
            records = [{obj: value} for value in value_list]
        else:
            records = list()
            if obj == 'ip':
                for ip in value_list:
                    hostname, alias_list, ipaddrlist = socket.gethostbyaddr(ip)
                    records.append({'ip': ip, 'hostname': hostname})
            elif obj == 'hostname':
                for hostname in value_list:
                    try:
                        ip = socket.gethostbyname(hostname)
                    except gaierror:
                        ip = None
                    records.append({'ip': ip, 'hostname': hostname})
            else:
                raise NotImplemented

        self.records(records, packet)

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @returns(Announce)
    def make_announces(self):
        """
        Returns:
            The Announce related to this object.
        """
        platform_name = self.get_platform_name()

        @returns(list)
        @announces_from_docstring(platform_name)
        def make_announces_impl():
            """
            class ip {
                const inet ip;
                hostname hostname;
    
                CAPABILITY(join);
                KEY(ip);
            };

            class hostname {
                const hostname hostname;
                ip ip;
    
                CAPABILITY(join);
                KEY(hostname);
            };
            """
        announces = make_announces_impl()
        return announces
