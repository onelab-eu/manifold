#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manage IPv4 and IPv6 addresses in Maxmind
#   https://code.google.com/p/pygeoip/wiki/Usage
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC

from types                                      import GeneratorType

from manifold.core.announce                     import Announce, announces_from_docstring
from manifold.core.fields                       import Fields
from manifold.gateways.object                   import Object
from manifold.gateways.maxmind.geoip_database   import MAXMIND_DAT_IPV4_ASN, MAXMIND_DAT_IPV4_CITY, MAXMIND_DAT_IPV4_COUNTRY
from manifold.gateways.maxmind.geoip_database   import MAXMIND_DAT_IPV6_ASN, MAXMIND_DAT_IPV6_CITY, MAXMIND_DAT_IPV6_COUNTRY
from manifold.util.log                          import Log
from manifold.util.type                         import accepts, returns
from manifold.util.predicate                    import eq, included

@returns(int)
def ip_get_family(ip):
    """
    Tests whether an IP address is IPv4 or IPv6
    Args:
        ip: A String containing an IP address
    Raises:
        ValueError: if the provided String is not an IP address
    Returns:
        4: if this is an IPv4 address
        6: if this is an IPv6 address
    """
    import socket

    try:
        s = socket.inet_pton(socket.AF_INET6, ip)
        return 6
    except socket.error:
        pass

    try:
        s = socket.inet_pton(socket.AF_INET, ip)
        return 4
    except socket.error:
        pass

    raise ValueError("Invalid ip = %s" % ip)

class Ip(Object):
    aliases = dict()

    @returns(GeneratorType)
    def get(self, query, annotation):
        """
        Retrieve an Ip
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise).
        Returns:
            A dictionnary containing the requested Object.
        """
        ip_list = []
        gateway = self.get_gateway()
        ip_list = query.get_filter().get_field_values('ip')

        if not ip_list:
            return

        # We don't really ask something sometimes...
        if query.get_fields() == Fields(['ip']):
            for ip in ip_list:
                print "yield", ip
                yield {'ip': ip}
        else:
            for ip in ip_list:
                if not ip:
                    continue
                ip_family = ip_get_family(ip)
                record = {"ip" : ip}
                select_all = query.get_fields().is_star()

    # I don't know why, those dat file cannot be loaded...
    #DISABLED|        # ASN
    #DISABLED|        if select_all or "as_num" in query.get_select():
    #DISABLED|            try:
    #DISABLED|                geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_ASN if ip_family == 4 else MAXMIND_DAT_IPV6_ASN)
    #DISABLED|                record.update(geoip.record_by_addr(ip))
    #DISABLED|            except Exception, e:
    #DISABLED|                Log.warning(e)
    #DISABLED|
    #DISABLED|        # Country
    #DISABLED|        if select_all:
    #DISABLED|            try:
    #DISABLED|                geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_COUNTRY if ip_family == 4 else MAXMIND_DAT_IPV6_COUNTRY)
    #DISABLED|                record.update(geoip.record_by_addr(ip))
    #DISABLED|            except Exception, e:
    #DISABLED|                Log.warning(e)

                # City
                if select_all or Fields(["city", "region_name", "area_code", "longitude", "country_code3", "latitude", "postal_code", "dma_code", "country_code", "country_name"]) & query.get_select():
                    try:
                        geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_CITY if ip_family == 4 else MAXMIND_DAT_IPV6_CITY)
                        record.update(geoip.record_by_addr(ip))
                    except Exception, e:
                        Log.warning(e)

                yield record

    @returns(Announce)
    def make_announce(self):
        """
        Returns:
            The Announce related to this object.
        """
        platform_name = self.get_gateway().get_platform_name()

        @returns(list)
        @announces_from_docstring(platform_name)
        def make_announces_impl():
            """
            // See record_by_addr
            class ip {
                const inet   ip;            /**< Ex: '64.233.161.99'    */
                const string city;          /**< Ex: 'Mountain View'    */
                const string region;        /**< Ex: 'A8'               */ 
                const string region_name;   /**< Ex: 'CA'               */
                const int    area_code;     /**< Ex: 650                */
                const double longitude;     /**< Ex: -122.0574          */
                const string country_code3; /**< Ex: 'USA'              */
                const double latitude;      /**< Ex: 37.419199999999989 */
                const string postal_code;   /**< Ex: '94043'            */
                const int    dma_code;      /**< Ex: 807                */
                const string country_code;  /**< Ex: 'US'               */
                const string country_name;  /**< Ex: 'United States'    */
                const string metro_code;    /**< Ex: '0'                */
                CAPABILITY(join);
                KEY(ip);
            };
            """
        announces = make_announces_impl()
        return announces[0]

