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
from manifold.core.fields               import Fields
from manifold.gateways                  import Gateway
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns 
from manifold.util.predicate            import eq, included

import adns
from time import time

# http://www.catonmat.net/blog/asynchronous-dns-resolution/
class AsyncResolver(object):
    def __init__(self, hosts, intensity=100):
        """
        hosts: a list of hosts to resolve
        intensity: how many hosts to resolve at once
        """
        self.hosts = hosts
        self.intensity = intensity
        self.adns = adns.init()

    def resolve(self):
        """ Resolves hosts and returns a dictionary of { 'host': 'ip' }. """
        resolved_hosts = {}
        active_queries = {}
        host_queue = self.hosts[:]

        def collect_results():
            for query in self.adns.completed():
                answer = query.check()
                host = active_queries[query]
                del active_queries[query]
                if answer[0] == 0:
                    ip = answer[3][0]
                    resolved_hosts[host] = ip
                elif answer[0] == 101: # CNAME
                    query = self.adns.submit(answer[1], adns.rr.A)
                    active_queries[query] = host
                else:
                    resolved_hosts[host] = None

        def finished_resolving():
            return len(resolved_hosts) == len(self.hosts)

        while not finished_resolving():
            while host_queue and len(active_queries) < self.intensity:
                host = host_queue.pop()
                query = self.adns.submit(host, adns.rr.A)
                active_queries[query] = host
            collect_results()

        return resolved_hosts

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
                    print "RESOLVING ip", ip
                    hostname, alias_list, ipaddrlist = socket.gethostbyaddr(ip)
                    records.append({'ip': ip, 'hostname': hostname})

            elif obj == 'hostname':
                # XXX NOT SURE IT IS ASYNC !!!
                ar = AsyncResolver(value_list, intensity=500)
                start = time()
                resolved_hosts = ar.resolve()
                end = time()
                print "It took %.2f seconds to resolve %d hosts." % (end-start, len(value_list))
                for hostname, ip in resolved_hosts.items():
                    # ip = None for hostname that could not be resolved
                    records.append({'ip': ip, 'hostname': hostname})

                # SLOW CODE !!!
                #for hostname in value_list:
                #    print "RESOLVING hostname", hostname
                #    try:
                #        ip = socket.gethostbyname(hostname)
                #    except socket.gaierror:
                #        ip = None
                #    records.append({'ip': ip, 'hostname': hostname})
            else:
                raise NotImplemented

        self.records(records, packet)

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @returns(Announces)
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
                const ip   ip;            /**< Ex: '64.233.161.99'    */
                const hostname hostname;    /**< Ex: 'www.hostname.com' */
    
                CAPABILITY(join);
                KEY(ip);
            };

            class hostname {
                const hostname hostname;    /**< Ex: 'www.hostname.com' */
                const ip   ip;            /**< Ex: '64.233.161.99'    */
    
                CAPABILITY(join);
                KEY(hostname);
            };
            """
        announces = make_announces_impl()
        return announces
