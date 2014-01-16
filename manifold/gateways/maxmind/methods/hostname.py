#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Manage hostnames in Maxmind 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

import GeoIP
from types                          import GeneratorType

from manifold.core.announce         import announces_from_docstring
from manifold.gateways.object       import Object
from manifold.util.type           	import accepts, returns 

class Hostname(Object):
    aliases = dict()

    @returns(GeneratorType)
    def get(self, query, annotation): 
        """
        Retrieve an Hostname 
        Args:
            query: The Query issued by the User.
            annotation: The corresponding Annotation (if any, None otherwise). 
        Returns:
            A dictionnary containing the requested Object.
        """

    @returns(list)
    def make_announces(self):
        """
        Returns:
            The list of Announce instances related to this object.
        """
        platform_name = self.get_gateway().get_platform_name()

        @returns(list)
        @announces_from_docstring("hostname")
        def make_announces_impl():
            """
            // See record_by_name
            class hostname {
                const string hostname;      /**< Ex: 'google.com'       */
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

                CAPABILITY(retrieve);
                KEY(hostname);
            };
            """
        announces = make_announces_impl()
        return announces
