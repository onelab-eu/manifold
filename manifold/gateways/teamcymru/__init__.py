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

# XXX dependencies : python-adns, python-ipy

import sys, time
from socket                             import *

from types                              import StringTypes
from manifold.core.announce             import Announces, announces_from_docstring
from manifold.core.fields               import Fields
from manifold.core.record               import Record, Records
from manifold.gateways                  import Gateway
from manifold.util.predicate            import eq, included
from manifold.util.type                 import accepts, returns 

# Related project:
# https://github.com/trolldbois/python-cymru-services/blob/master/cymru/ip2asn/dns.py



CRLF = "\r\n"

def compare(d1, d2, arr):
    for a in arr:
        if d1[a] != d2[a]:
            return False
    return True

class TeamCymruGateway(Gateway):

    __gateway_name__ = "tc"

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this gateway.
        """
        return "<TeamCymruGateway>"

    def parse_teamcymru(self, ips):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect(("whois.cymru.com", 43))
        file = sock.makefile("rb") # buffered

        sock.send('begin' + CRLF)
        # for fields != ip asn as_name 
        sock.send('verbose' + CRLF)
        for ip in ips:
            sock.send(ip + CRLF)
        sock.send('end' + CRLF)

        output = []
        line = file.readline().strip()
        while True:
            line = file.readline().strip()
            if not line:
                break
            asn, ip, prefix, cc, reg, alloc, asname = map(lambda x: x.strip(), line.split('|'))

            dic = {'ip': ip, 'asn': asn, 'prefix': prefix, 'country_code': cc, 'as_registry': reg, 'as_allocated': alloc, 'as_name': asname}
            output.append(dic)
        
        return output

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()

        obj = query.get_object()
        if obj == 'ip':
            token_key = 'ip'
            qtype = 'IP' # IP6 ??
        elif obj == 'as':
            token_key = 'asn'
            qtype= 'ASN'
        else:
            raise NotImplemented
        
        value_list = query.get_filter().get_field_values(token_key)

        # We don't really ask something sometimes...
        if query.get_fields() == Fields(['ip']):
            records = [{'ip': value} for value in value_list]
        else:

            # Use the nice python-cymru-services package
            # https://github.com/trolldbois/python-cymru-services
            if len(value_list) > 20:
                from .cymru.ip2asn.dns import WhoisClient as ip2asn
                client = ip2asn()
                ret = client.lookupmany_dict(value_list, qType=qtype)
            else:
                from .cymru.ip2asn.dns import DNSClient as ip2asn
                client = ip2asn()
                ret = client.lookupmany_dict(value_list, qType=qtype)

            records = list()
            for token_value, rec in ret.items():
                dic = dict()
                for key in ['asn', 'prefix', 'cc', 'lir', 'date']:
                    dic[key] = getattr(rec, key)
                owner = getattr(rec, 'owner', None)
                if owner:
                    dic['as_name'] = owner
                dic[token_key] = token_value
                records.append(Record(dic))

        self.records(records, packet)

            
        # We used to work with our own implementation...
        #self.records(self.parse_teamcymru(ip_list))

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
            // See record_by_addr
            class ip {
                const inet   ip;            /**< Ex: '132.227.1.1'      */
                as           asn;           /**< Ex: '1307'             */
                const string prefix;        /**< Ex: '132.227.0.0/16'   */
                const string cc;            /**< Ex:  'FR'              */
                const string lir;           /**< Ex: 'ripencc'          */
                const string date;          /**< Ex: '2003-11-04'       */

                CAPABILITY(join);
                KEY(ip);
            };

            class as {
                const string asn;           /**< Ex: '5511'             */
                const string as_name;       /**< Ex: 'RENATER'          */

                CAPABILITY(join);
                KEY(asn);
            };
            """
        announces = make_announces_impl()
        return announces

#const string country_code;  /**< Ex: 'USA'              */
#const string as_registry;   /**< Ex:                    */
#const string as_allocated;  /**< Ex:                    */
#const string as_name;       /**< Ex:                    */
