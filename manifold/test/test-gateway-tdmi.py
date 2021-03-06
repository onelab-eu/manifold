#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Tests for TDMI Gateway
#
# Usage:
#  ./test-gateway-tdmi.py
#  ./test-gateway-tdmi.py -d manifold -L DEBUG
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import sys

from manifold.gateways.tdmi        import TDMIGateway
from manifold.core.query           import Query
from manifold.core.result_value    import ResultValue
from manifold.core.router          import Router 
from manifold.util.storage         import DBStorage
from manifold.util.type            import returns, accepts 
from manifold.util.log             import Log
from manifold.util.options         import Options
from manifold.util.predicate       import Predicate, eq

Options().parse()

@accepts(dict)
def print_record(record):
    """
    Process a record fetched by the gateway
    Args:
        record: A dictionnary storing the fetched record
    """
    if record:
        print "{"
        for field_name in sorted(record.keys()):
            print "\t%s: %s" % (field_name, record[field_name])
        print "}"
    else:
        print "END"

@returns(Router)
def make_tdmi_router():
    """
    Prepare a TDMI router/
    Returns:
        The corresponding Router instance.
    """
    # Fetch tdmi configuration from myslice storage
    platforms = DBStorage.execute(Query().get("platform").filter_by(Predicate("platform", eq, "tdmi")), format = "dict")
    if len(platforms) == 1:
        platform = platforms[0] 
    else:
        # TODO: we should use this account
        #'db_user'     : 'guest@top-hat.info'
        #'db_password' : 'guest'
        print """No information found about TDMI in the storage, you should run:

        manifold-add-platform "tdmi" "Tophat Dedicated Measurement Infrastructure" "TDMI" "none" '{"db_host": "132.227.62.103", "db_port": 5432, "db_user": "postgres", "db_password": null, "db_name": "tophat", "name" : "TopHat team", "mail_support_address" : "xxx@xxx" }' 1
        """
        sys.exit(-1)

    # Our Forwarder does not need any capability since pgsql is
    return Router(platforms)

@accepts(Router, Query)
def run_query(router, query, execute = True):
    """
    Forward a query to the router and dump the result to the standard outpur
    Params:
        router: The router instance related to TDMI
        query: The query instance send to the TDMI's router
        execute: Execute the Query on the TDMIGateway
    """

    print "*" * 80
    print query
    print "*" * 80
    print "=" * 80
    result_value = router.forward(query, execute = execute)
    if execute:
        if result_value["code"] == ResultValue.SUCCESS:
            for record in result_value["value"]:
                print_record(record)
        else:
            Log.error("Failed to run query:\n\n%s" % query)

# Print metadata stored in the router
@accepts(Router)
def dump_routing_table(router):
    for platform, announces in router.metadata.items():
        print "*** Platform %s:" % platform
        for announce in announces:
            table = announce.get_table()
            print ">> %r (cost %r)\n%s\n%s" % (table, announce.get_cost(), table, table.get_capabilities())

router = make_tdmi_router()
#dump_routing_table(router)

queries = [
    # Query traceroute
    # TODO should work without querying src_ip
    Query(
        action  = "get",
        object  = "traceroute",
        filters =  [
            ["agent_id",       "=", 11824],
            ["destination_id", "=", 1417]
            #["destination_id", "=", [1416, 1417]]
        ],
        fields  = [
#            "src_ip", "dst_ip",
#            "agent.ip",   "destination.ip",
            "hops.ip", "hops.ttl"#, "hops.hostname", "timestamp"
        ],
        timestamp = "2012-09-09 14:30:09"
    ),

    # Query agent 
    Query(
        action  = "get",
        object  = "agent",
        filters =  [["agent_id", "=", 11824]],
        fields  = ["agent_id", "ip"]
    ),

    # Query destination 
    Query(
        action  = "get",
        object  = "destination",
        filters =  [["destination_id", "=", 1417]],
        fields  = ["destination_id", "ip"]
    ),

    # Query traceroute JOIN agent
    Query(
        action  = "get",
        object  = "traceroute",
        filters =  [
            ["agent_id",       "=", 11824],
            ["destination_id", "=", 1417]
            #["destination_id", "=", [1416, 1417]]
        ],
        fields  = [
            "agent.ip",       "src_ip", "agent.hostname",
            "destination.ip", "dst_ip", "destination.hostname",
#            "hops.ip", "hops.ttl"
        ],
        timestamp = "2012-09-09 14:30:09"
    )
]

#for query in queries:
#    run_query(router, query)
#run_query(router, queries[1])
run_query(router, queries[0])


