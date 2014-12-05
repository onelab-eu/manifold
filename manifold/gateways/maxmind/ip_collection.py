#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manage IPv4 and IPv6 addresses in Maxmind
#   https://code.google.com/p/pygeoip/wiki/Usage
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC

from types                          import GeneratorType

from geoip_database                 import MAXMIND_DAT_IPV4_ASN, MAXMIND_DAT_IPV4_CITY, MAXMIND_DAT_IPV4_COUNTRY
from geoip_database                 import MAXMIND_DAT_IPV6_ASN, MAXMIND_DAT_IPV6_CITY, MAXMIND_DAT_IPV6_COUNTRY

from manifold.core.field_names      import FieldNames
from manifold.gateways.object       import ManifoldCollection
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns
from manifold.util.predicate        import eq, included

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

class IPCollection(ManifoldCollection):
    """
    // See record_by_addr
    class ip {
        const ip     ip;            /**< Ex: '64.233.161.99'    */
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

    aliases = dict()

    @returns(GeneratorType)
    def get(self, packet):
        """
        Retrieve an Ip
        Args:
            packet:
        Returns:
            A dictionnary containing the requested Object.
        """
        destination = packet.get_destination()
        ip_list = []
        gateway = self.get_gateway()
        ip_list = destination.get_filter().get_field_values('ip')

        if not ip_list:
            return

        # We don't really ask something sometimes...
        if destination.get_select() == FieldNames(['ip']):
            for ip in ip_list:
                #print "yield", ip
                yield {'ip': ip}
        else:
            for ip in ip_list:
                if not ip:
                    continue
                ip_family = ip_get_family(ip)
                record = {"ip" : ip}
                select_all = destination.get_select().is_star()

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
                if select_all or FieldNames(["city", "region_name", "area_code", "longitude", "country_code3", "latitude", "postal_code", "dma_code", "country_code", "country_name"]) & destination.get_field_names():
                    try:
                        geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_CITY if ip_family == 4 else MAXMIND_DAT_IPV6_CITY)
                        record.update(geoip.record_by_addr(ip))
                    except Exception, e:
                        Log.warning(e)

                yield record
