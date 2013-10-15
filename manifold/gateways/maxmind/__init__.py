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

#import GeoIP

from manifold.gateways.gateway          import Gateway
from manifold.operators                 import LAST_RECORD
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

    @returns(bool)
    def check_init(self):
        """
        Check whether MaxMindGateway can be constructed.
        """
        try:
            import GeoIP
        except ImportError, e:
            Log.error(e)

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

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, account_config = None, format = "dict", from_node = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            account_config: A dictionnary containing the user's account config.
                In pratice, this is the result of the following query (run on the Storage)
                SELECT config FROM local:account WHERE user_id == user.user_id
            format: A String specifying in which format the Records must be returned.
            from_node : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        Gateway.forward(self, query, callback, is_deferred, execute, user, account_config, format, receiver)
        identifier = from_node.get_identifier() if from_node else None

        #assert timestamp == 'latest'
        #assert set(input_filter.keys()).issubset(['ip', 'hostname'])
        #assert set(output_fields).issubset(allowed_fields)
        print "W: MaxMind faking data until python-geoip is installed"
        self.started = True
        # XXX We never stop gateways even when finished. Investigate ?

        if not 'ip' in query.filters and not 'hostname' in query.filters:
            raise Exception, "MaxMind is an ONJOIN platform only"

        rows = list()

        # HARDCODED dummy cities
        for i in query.filters['ip']:
            rows.append({'ip': i, 'city': 'TEMP'})

        # HARDCODED dummy hostnames
        for h in query.filters['hostname']:
            rows.append({'hostname': h, 'city': 'TEMP'})

        for row in rows:
            self.send(row, callback, identifier)
        self.send(LAST_RECORD, callback, identifier)

        self.success(from_node, query)

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
