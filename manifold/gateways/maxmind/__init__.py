from manifold.gateways import Gateway
import GeoIP

geo_fields = {
    'city': 'city',
    'country_code': 'country_code',
    'country': 'country_name',
    'region_code': 'region',
    'region': 'region_name',
    'latitude': 'latitude',
    'longitude': 'longitude'
}
# 'country_code3' 'postal_code' 'area_code' 'time_zone' 'metro_code'

allowed_fields = ['ip', 'hostname']
allowed_fields.extend(geo_fields.keys())


class MaxMindGateway(Gateway):
    __gateway_name__ = 'maxmind'

    def __str__(self):
        return "<MaxMindGateway %s>" % self.query

    def start(self):
        #assert timestamp == 'latest'
        #assert set(input_filter.keys()).issubset(['ip', 'hostname'])
        #assert set(output_fields).issubset(allowed_fields)
        print "W: MaxMind faking data until python-geoip is installed"
        self.started = True
        # XXX We never stop gateways even when finished. Investigate ?

        if not 'ip' in self.query.filters and not 'hostname' in self.query.filters:
            raise Exception, "MaxMind is an ONJOIN platform only"
        for i in self.query.filters['ip']:
            self.callback({'ip': i, 'city': 'TEMP'})
        for h in self.query.filters['hostname']:
            self.callback({'hostname': h, 'city': 'TEMP'})
        self.callback(None)
        return 

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
