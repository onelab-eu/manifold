#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MaxMind platforms
# http://www.maxmind.com/
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import GeoIP

from manifold.core.record               import Record
from manifold.gateways                  import Gateway
from manifold.util.log                  import Log
from manifold.util.type                 import returns, accepts 

geo_fields = {
    "city"         : "city",
    "country_code" : "country_code",
    "country"      : "country_name",
    "region_code"  : "region",
    "region"       : "region_name",
    "latitude"     : "latitude",
    "longitude"    : "longitude"
}
# 'country_code3' 'postal_code' 'area_code' 'time_zone' 'metro_code'

allowed_fields = ['ip', 'hostname']
allowed_fields.extend(geo_fields.keys())

class MaxMindGateway(Gateway):
    __gateway_name__ = 'maxmind'

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

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        self.check_receive(packet)
        query = packet.get_query()
        where = query.get_where()

        Log.warning("MaxMind faking data until python-geoip is installed")

        if not "ip" in where and not "hostname" in where:
            self.error(packet, "MaxMind is an ONJOIN platform only")
            return

        rows = list()

        # HARDCODED dummy cities
        for ip in where:
            rows.append({"ip": ip, "city": "TEMP"})

        # HARDCODED dummy hostnames
        for hostname in where:
            rows.append({"hostname": hostname, "city": "TEMP"})

        self.records(packet, rows)

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
