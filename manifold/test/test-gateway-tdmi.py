#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Tests for TDMI Gateway
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
    # TODO select().where().one()
    # Fetch tdmi configuration from myslice storage
    platforms = DBStorage.execute(Query().get("platform"), format = "object")
    try:
        platform = [platform for platform in platforms if platform.name == "tdmi"][0]
    except:
        # TODO: we should use this acount
        #'db_user'     : 'guest@top-hat.info'
        #'db_password' : 'guest'
        print """No information found about TDMI in the storage, you should run:

        myslice-add-platform "tdmi" "Tophat Dedicated Measurement Infrastructure" "TDMI" "none" '{"db_host": "132.227.62.103", "db_port": 5432, "db_user": "postgres", "db_password": null, "db_name": "tophat", "name" : "TopHat team", "mail_support_address" : "xxx@xxx" }' 1
        """
        sys.exit(-1)

    # Our Forwarder does not need any capability since pgsql is
    return Router(platform)

@returns(ResultValue)
@accepts(Router, Query)
def run_query(router, query):
    """
    Forward a query to the router and dump the result to the standard outpur
    Params:
        router: The router instance related to TDMI
        query: The query instance send to the TDMI's router
    """
    print "=" * 80
    return router.forward(query)


# Print metadata stored in the router
@accepts(Router)
def dump_routing_table(router):
    for platform, announces in router.metadata.items():
        print "*** Platform %s:" % platform
        for announce in announces:
            print ">> %r (cost %r)" % (announce.get_table(), announce.get_cost())
            print "%s\n" % announce.get_table()


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
            "src_ip", "dst_ip",
            "agent",   "destination",
            "hops.ip", "hops.ttl"#, "hops.hostname", "timestamp"
        ],
        timestamp = "2012-09-09 14:30:09"
    ),

    # Query agent 
    Query(
        action  = "get",
        object  = "agent",
        filters =  [["agent_id", "=", 11824]],
        fields  = ["agent_id", "ip", "hostname", "platform"]
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
            "agent", "agent.ip", "src_ip", "agent.hostname"
            "destination", "destination.ip", "dst_ip", "destination.hostname"
        ],
        timestamp = "2012-09-09 14:30:09"
    )
]

for query in queries:
    result_value = run_query(router, query)
    if result_value["code"] == ResultValue.SUCCESS:
        for record in result_value["value"]:
            print_record(record)
    else:
        Log.error("Failed to run query:\n\n%s" % query)
