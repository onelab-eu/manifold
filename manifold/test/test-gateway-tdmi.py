#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.gateways.tdmi import TDMIGateway
from manifold.core.query    import Query
#from manifold.util.log      import Logger

#l = Logger("test_tdmi")

def tdmi_callback(record):
    """
    Process a record fetched by the gateway
    Args:
        record: A Record instance storing the fetched record
    """
    print "TDMI: %r" % record

#cfg['SCHEME'] = 'password'
#cfg['USERNAME'] = 'guest@top-hat.info'
#cfg['PASSWORD'] = 'guest'

# Default configuration
config = {
    "db_user"              : "postgres",
#    "db_host"              : 132.227.62.103, 
    "db_password"          : None,
    "db_name"              : "tophat",
#    "db_port"              : 5432,
    "name"                 : "TopHat team",
    "mail_support_address" : "xxx@xxx" 
    ""
}

# Default query 
query = Query(
    action = "get",
    object = "traceroute",
    filters =  [
        ["agent_id",       "=", 11824],
        ["destination_id", "=", 1417]
        #["destination_id", "=", [1416, 1417]]
    ],
    fields = [
    #    "src_ip", "dst_ip", "src_hostname", "dst_hostname",
        "agent",   "destination",
        "hops.ip", "hops.ttl", "hops.hostname", "timestamp"
    ],
    timestamp = "2012-09-09 14:30:09"
)

# Prepare the TDMI gateway
gw = TDMIGateway(
    router      = None,
    platform    = "tdmi",
    query       = query,
    config      = config,
    user_config = None,
    user        = None
)
gw.set_callback(tdmi_callback)
gw.start()

#from tdmi.core.query  import Query
#from tdmi.core.server import TDMIServer
#
#def run_test(query):
#    s = TDMIServer()
#    rows = s.execute(query)
#    for row in rows:
#        print row
#
# run_test(q)

# test 2
#print 2, 100*"="
#select = ["agent_id", "src_ip", "destination_id", "dst_ip", "first", "last", "timestamp", "hop_count"]
#run_test(select, where, ts)
#
## test 3
#print 3, 100*"="
#ts = "latest"
#where = {
#    "src_ip": "141.22.213.34",
#    "dst_ip": ["139.91.90.239", "195.116.60.211"]
#}
#run_test(select, where, ts)
#
