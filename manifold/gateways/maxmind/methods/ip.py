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
from manifold.gateways.object                   import Object
from manifold.gateways.maxmind.geoip_database   import MAXMIND_DAT_IPV4_CITY
from manifold.util.log                          import Log
from manifold.util.type                         import accepts, returns 

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
        ip = None
        gateway = self.get_gateway()
        where = query.get_where()
        predicates = [predicate for predicate in query.get_where().get("ip") if predicate.get_str_op() == "=="]

        if len(predicates) != 1: 
            raise RuntimeError(
                """
                The WHERE clause (%s) must have exaclty one Predicate '==' involving 'ip' field.
                Matching Predicate(s): {%s}
                """ % ', '.join(predicates)
            )
        ip = predicates[0].get_value()
        record = dict()

        if "as_num" in query.get_select():
            geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_ASN)
            Log.warning("not yet implemented")

        # City
#        if set(["city", "region_name", "area_code", "longitude", "country_code3", "latitude", "postal_code", "dma_code", "country_code", "country_name"]) & query.get_select():
        geoip = gateway.get_geoip(MAXMIND_DAT_IPV4_CITY)
        records = [geoip.record_by_addr(ip)]
        for record in records:
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
                const string region_name;   /**< Ex: 'CA'               */
                const int    area_code;     /**< Ex: 650                */
                const double longitude;     /**< Ex: -122.0574          */
                const string country_code3; /**< Ex: 'USA'              */
                const double latitude;      /**< Ex: 37.419199999999989 */
                const string postal_code;   /**< Ex: '94043'            */
                const int    dma_code;      /**< Ex: 807                */
                const string country_code;  /**< Ex: 'US'               */
                const string country_name;  /**< Ex: 'United States'    */

                CAPABILITY(retrieve, fullquery);
                KEY(ip);
            }; 
            """
        announces = make_announces_impl()
        return announces[0]

