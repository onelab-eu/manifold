#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MaxMind - Geolite platform
#   http://www.maxmind.com/
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Auge       <jordan.auge@lip6.fr>
#
# Copyright (C) UPMC 

# TODO: it would be nice to periodically re-download GeoIP databases
# iif updated (e.g. by using "wget -N url")
# http://dev.maxmind.com/geoip/legacy/install/country/

import os

from manifold.gateways.maxmind.geoip_database   import check_filename_dat, get_dat_basenames, install_dat, MAXMIND_DIR
from manifold.gateways                          import Gateway
from manifold.util.log                          import Log
from manifold.util.type                         import returns, accepts 

class MaxMindGateway(Gateway):

    __gateway_name__ = "maxmind"

    from manifold.gateways.maxmind.methods.ip       import Ip
    from manifold.gateways.maxmind.methods.hostname import Hostname

    METHOD_MAP = {
        "ip"       : Ip,
        "hostname" : Hostname
    }

    def __init__(self, interface, platform_name, platform_config): 
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the platform related to this Gateway or None.
            config: A dictionnary containing the configuration related to this Gateway.
        """
        super(MaxMindGateway, self).__init__(interface, platform_name, platform_config)
        dat_basenames = get_dat_basenames()

        self.map_dat_geoips = dict()

        # Install dat files
        for dat_basename in dat_basenames:
            try:
                install_dat(dat_basename, False)
            except Exception, e:
                Log.warning("%s: Cannot install %s database: %s" % (self.get_platform_name(), dat_basename, e))

    def get_geoip(self, dat_basename):
        """
        Test whether a dat file is available.
        Args:
            dat_basename: A String containing the absolute path of a dat file.
        Raises:
            ValueError: If dat_filename refers to an dat filename not supported.
            ImportError: If GeoIP is not properly installed.
        Returns:
            The corresponding GeoIP instance.
        """
        try:
            import GeoIP
        except ImportError, e:
            raise ImportError("%s requires python-geoip package (%s)" % (self.get_platform_name(), e))

        check_filename_dat(dat_basename)
        try:
            geoip = self.map_dat_geoips[dat_basename]
        except KeyError:
            # This database is not yet loaded
            dat_filename = os.path.join(MAXMIND_DIR, dat_basename)
            #Log.tmp("dat_filename = %s" % dat_filename)
            geoip = GeoIP.open(dat_filename, GeoIP.GEOIP_STANDARD)
            self.map_dat_geoips[dat_basename] = geoip
        return geoip

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        table_name = query.get_from()

        records = None 
        if table_name in MaxMindGateway.METHOD_MAP.keys():
            instance = MaxMindGateway.METHOD_MAP[table_name](self)
            records = instance.get(query, packet.get_annotation())
        else:
            raise RuntimeError("Invalid object %s" % table_name) 
        self.records(records, packet)

    #---------------------------------------------------------------------------
    # Metadata 
    #---------------------------------------------------------------------------

    @returns(list)
    def make_announces(self):
        """
        Build announces by querying postgresql's information schema
        Returns:
            The list of corresponding Announce instances
        """
        # Ex:  https://code.google.com/p/pygeoip/wiki/Usage
        # Doc: https://code.google.com/p/pygeoip/downloads/list
        announces = list()
        for instance in MaxMindGateway.METHOD_MAP.values():
            announces.append(instance(self).make_announce())
        return announces
