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

from manifold.gateways.object       import ManifoldCollection
from manifold.util.type           	import accepts, returns 

class HostnameCollection(ManifoldCollection):
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

    @returns(GeneratorType)
    def get(self, packet):
        """
        Retrieve an Hostname 
        Args:
            packet:
        Returns:
            A dictionnary containing the requested Object.
        """

