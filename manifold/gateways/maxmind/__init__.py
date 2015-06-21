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

from manifold.core.announce                     import Announces
from manifold.gateways.maxmind.geoip_database   import check_filename_dat, get_dat_basenames, install_dat, MAXMIND_DIR
from manifold.gateways                          import Gateway
from manifold.util.log                          import Log
from manifold.util.type                         import returns, accepts


from ip_collection                              import IPCollection
from hostname_collection                        import HostnameCollection

class MaxMindGateway(Gateway):

    __gateway_name__ = "maxmind"

    def __init__(self, router = None, platform_name = None, **platform_config):
        Gateway.__init__(self, router, platform_name, **platform_config)

        dat_basenames = get_dat_basenames()

        self.map_dat_geoips = dict()

        # Install dat files
        for dat_basename in dat_basenames:
            try:
                install_dat(dat_basename, False)
            except Exception, e:
                Log.warning("%s: Cannot install %s database: %s" % (self.get_platform_name(), dat_basename, e))

        self.register_collection(IPCollection())
        #self.register_collection(HostnameCollection())

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
