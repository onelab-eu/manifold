#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MaxMind - Geolite platform
#   http://www.maxmind.com/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import GeoIP, os
from types                              import StringTypes
from urllib                             import urlretrieve

from manifold.core.announce             import announces_from_docstring
from manifold.gateways                  import Gateway
from manifold.util.filesystem           import check_writable_directory, check_readable_file, mkdir
from manifold.util.log                  import Log
from manifold.util.type                 import returns, accepts 

#DEPRECATED|geo_fields = {
#DEPRECATED|    "city"         : "city",
#DEPRECATED|    "country_code" : "country_code",
#DEPRECATED|    "country"      : "country_name",
#DEPRECATED|    "region_code"  : "region",
#DEPRECATED|    "region"       : "region_name",
#DEPRECATED|    "latitude"     : "latitude",
#DEPRECATED|    "longitude"    : "longitude"
#DEPRECATED|}
#DEPRECATED|# 'country_code3' 'postal_code' 'area_code' 'time_zone' 'metro_code'
#DEPRECATED|
#DEPRECATED|allowed_fields = ['ip', 'hostname']
#DEPRECATED|allowed_fields.extend(geo_fields.keys())

MAXMIND_DIR          = "/usr/local/share/geolite/"
MAXMIND_DAT_URL      = "http://dev.maxmind.com/geoip/legacy/geolite"

MAXMIND_DAT_IPV4_ASN     = "GeoIPASNum.dat.gz"     # GeoLite ASN
MAXMIND_DAT_IPV6_ASN     = "GeoIPASNumv6.dat.gz"   # GeoLite ASN IPv6
MAXMIND_DAT_IPV4_COUNTRY = "GeoIP.dat.gz"          # GeoLite Country
MAXMIND_DAT_IPV6_COUNTRY = "GeoIPv6.dat.gz"        # GeoLite Country IPv6 (beta)
MAXMIND_DAT_IPV4_CITY    = "GeoLiteCity.dat.gz"    # GeoLite City
MAXMIND_DAT_IPV6_CITY    = "GeoLiteCityv6.dat.gz"  # GeoLite City IPv6

MAXMIND_DAT_BASENAMES = [
    MAXMIND_DAT_IPV4_ASN,
    MAXMIND_DAT_IPV6_ASN,
    MAXMIND_DAT_IPV4_COUNTRY,
    MAXMIND_DAT_IPV6_COUNTRY,
    MAXMIND_DAT_IPV4_CITY,
    MAXMIND_DAT_IPV6_CITY
]

class MaxMindGateway(Gateway):
    __gateway_name__ = "maxmind"

    @staticmethod
    @accepts(StringTypes)
    def check_dat_filename(dat_filename):
        """
        Test whether a dat file is available.
        Args:
            dat_filename: A String containing the absolute path of a dat file.
        """
        assert isinstance(dat_filename, StringTypes),\
            "Invalid dat_filename = %s (%s) " % (dat_filename, type(dat_filename))

        basename = os.path.basename(dat_filename)
        allowed_basenames = MaxMindGateway.get_dat_basenames()
        if not basename in allowed_basenames:
            raise ValueError("%s is not supported. Supported values are {%s}" % ", ".join(allowed_basenames))

    @staticmethod
    @returns(bool)
    @accepts(StringTypes)
    def is_dat_available(dat_filename):
        """
        Test whether a dat file is available.
        Args:
            dat_filename: A String containing the absolute path of a dat file.
        Raises:
            ValueError: If dat_filename refers to an dat filename not supported.
        Returns:
            True iif this dat file is available, False otherwise.
        """
        ret = True
        MaxMindGateway.check_dat_filename(dat_filename)
        try:
            check_readable_file(dat_filename)
        except:
            ret = False
        return ret

    @staticmethod
    @returns(list)
    def get_dat_basenames():
        """
        Returns:
            A list of String where each String corresponds to a
            supported MaxMind dat files.
        """
        return MAXMIND_DAT_BASENAMES

    @staticmethod
    @accepts(StringTypes, bool)
    def install_dat(dat_basename, overwrite):
        """
        Install a MaxMind dat file on the local filesystem.
        Args:
            dat_basename: A String contained in MaxMind.get_dat_basenames() 
            overwrite: Pass False if the eventual existing file can be kept,
                True otherwise.
        Raises:
            RuntimeError: in case of failure
        """
        try:
            MaxMindGateway.check_dat_filename(dat_basename)
            dat_filename = os.path.join(MAXMIND_DIR, dat_basename)

            # Do not download dat file if already cached
            if not overwrite and MaxMindGateway.is_dat_available(dat_filename):
                Log.info("Dat file found: %(dat_filename)s" % locals())
                return

            # mkdir MAXMIND_DIR
            try:
                check_writable_directory(MAXMIND_DIR)
            except:
                Log.info("Creating '%s' directory" % MAXMIND_DIR)
                mkdir(MAXMIND_DIR)

            # wget dat_filename
            dat_url = "%(maxmind_dat_url)s/%(basename_dat)s" % {
                "maxmind_dat_url" : MAXMIND_DAT_URL,
                "basename_dat"    : dat_basename
            }
            Log.info("Downloading '%(dat_url)s' into '%(dat_filename)s'" % locals())
            urlretrieve(dat_url, dat_filename)
        except Exception, e:
            raise RuntimeError(e)

    def __init__(self, interface, platform, config = None):
        """
        Constructor
        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            config: A dictionnary containing the configuration related to this Gateway.
                It may contains the following keys:
                "name" : name of the platform's maintainer. 
                "mail" : email address of the maintainer.
        """
        super(MaxMindGateway, self).__init__(interface, platform, config)
        for dat_basename in MaxMindGateway.get_dat_basenames():
            MaxMindGateway.install_dat(dat_basename, False)

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_receive(packet)
        query = packet.get_query()
        records = list()
        self.records(packet, records)



#        where = query.get_where()
#
#        Log.warning("MaxMind faking data until python-geoip is installed")
#
#        if not "ip" in where and not "hostname" in where:
#            self.error(packet, "MaxMind is an ONJOIN platform only")
#            return
#
#        rows = list()
#
#        # HARDCODED dummy cities
#        for ip in where:
#            rows.append({"ip": ip, "city": "TEMP"})
#
#        # HARDCODED dummy hostnames
#        for hostname in where:
#            rows.append({"hostname": hostname, "city": "TEMP"})
#
#        self.records(packet, rows)
#
#
#        gi = GeoIP.open("/usr/local/share/GeoIP/GeoLiteCity.dat",GeoIP.GEOIP_STANDARD)
#
#        output = []
#
#        if 'ip' in input_filter:
#            ips = input_filter['ip']
#            if not isinstance(ips, list):
#                ips = [ips]
#            for ip in ips:
#                gir = gi.record_by_addr(ip)
#                d = {}
#                for o in output_fields:
#                    if o == 'ip':
#                        d[o] = ip
#                    elif o == 'hostname':
#                        d[o] = None
#                    else: # We restrict output fields hence it's OK.
#                        d[o] = gir[geo_fields[o]] if gir else None
#                output.append(d)
#        
#        if 'hostname' in input_filter:
#            hns = input_filter['hostname']
#            if not isinstance(hns, list):
#                hns = [hns]
#            for hn in hns:
#                gir = gi.record_by_name(hn)
#                d = {}
#                for o in output_fields:
#                    if o == 'ip':
#                        d[o] = None
#                    elif o == 'hostname':
#                        d[o] = hn
#                    else: # We restrict output fields hence it's OK.
#                        d[o] = gir[geo_fields[o]] if gir else None
#                output.append(d)
#        
#        if 'city' in output_fields:
#            for o in output:
#                if o['city']:
#                    o['city'] = o['city'].decode('iso-8859-1')
#        for record in output:
#            self.callback(record)
#        self.callback(None)

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
        
        @announces_from_docstring(platform_name)
        def make_announces_impl():
            """
            // See record_by_addr
            class ip {
                inet   ip;            /**< Ex: '64.233.161.99' */
                string city;          /**< Ex: 'Mountain View' */
                string region_name,   /**< Ex: 'CA' */
                int    area_code;     /**< Ex: 650 */
                double longitude;     /**< Ex: -122.0574 */
                string country_code3; /**< Ex: 'USA' */
                double latitude;      /**< Ex: 37.419199999999989 */
                string postal_code;   /**< Ex: '94043' */
                int    dma_code       /**< Ex: 807 */
                string country_code   /**< Ex: 'US' */
                string country_name   /**< Ex: 'United States' */

                CAPABILITY(retrieve);
                KEY(table);
            }; 

            // See record_by_name
            class hostname {

            }
            """

        return make_announces_impl()
