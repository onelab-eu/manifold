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

class IPCollection(ManifoldCollection):
    """
    class ip {
        const ip   ip;            /**< Ex: '64.233.161.99'    */
        const hostname hostname;    /**< Ex: 'www.hostname.com' */

        CAPABILITY(join);
        KEY(ip);
    };
    """

    def get(self, packet):
        destination = packet.get_destination()

        obj = destination.get_object()
        value_list = destination.get_filter().get_field_values(obj)

        # We don't really ask something sometimes...
        if destination.get_field_names() == FieldNames([obj]):
            records = [{obj: value} for value in value_list]
        else:
            records = list()
            for ip in value_list:
                print "RESOLVING ip", ip
                hostname, alias_list, ipaddrlist = socket.gethostbyaddr(ip)
                records.append({'ip': ip, 'hostname': hostname})

        self.get_gateway().records(records, packet)

class HostnameCollection(ManifoldCollection):
    """
    class hostname {
        const hostname hostname;    /**< Ex: 'www.hostname.com' */
        const ip   ip;            /**< Ex: '64.233.161.99'    */

        CAPABILITY(join);
        KEY(hostname);
    };
    """

    def get(self, packet):
        destination = packet.get_destination()

        obj = destination.get_object()
        value_list = destination.get_filter().get_field_values(obj)

        # We don't really ask something sometimes...
        if destination.get_field_names() == FieldNames([obj]):
            records = [{obj: value} for value in value_list]
        else:
            records = list()
            # XXX NOT SURE IT IS ASYNC !!!
            ar = AsyncResolver(value_list, intensity=500)
            start = time()
            resolved_hosts = ar.resolve()
            end = time()
            print "It took %.2f seconds to resolve %d hosts." % (end-start, len(value_list))
            for hostname, ip in resolved_hosts.items():
                # ip = None for hostname that could not be resolved
                records.append({'ip': ip, 'hostname': hostname})

        self.get_gateway().records(records, packet)


class DNSGateway(Gateway):

    __gateway_name__ = "dns"

    def __init__(self, router = None, platform_name = None, platform_config = None):
        Gateway.__init__(self, router, platform_name, platform_config)

        self.register_collection(IPCollection())
        self.register_collection(HostnameCollection())

#    def get_value_list(self, token, query):
#        assert token in ['ip', 'hostname']
#
#        value_list = []
#        where = query.get_filter()
#
#        predicate_list = []
#        predicate_list.extend(where.get(token))
#        predicate_list.extend(where.get((token,)))
#
#        get_value = lambda value: value[0] if isinstance(value, tuple) else value
#
#        for predicate in predicate_list:
#            key, op, value = predicate.get_tuple()
#            if op is eq:
#                value_list.append(get_value(value))
#            elif op is included:
#                value_list.extend([get_value(v) for v in value])
#            else:
#                raise RuntimeError(
#                    """
#                    The WHERE clause (%s) must have exaclty one Predicate '==' involving 'ip' field.
#                    Matching Predicate(s): {%s}
#                    """ % (
#                        where,
#                        ', '.join(predicates)
#                    )
#                )
#        return value_list
