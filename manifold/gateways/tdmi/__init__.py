#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This Gateway allows to access to Tophat Dedicated Measurement
# Infrastructure (TDMI).
# https://www.top-hat.info/download/tdmi-description.txt
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Jordan Augé      <jordan.auge@lip6.fr>
#
# Copyright (C) 2013 UPMC 

import re

from manifold.core.announce         import Announces
from manifold.core.field            import Field 
from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway
from manifold.util.type             import accepts, returns 
from manifold.util.log              import Log

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):
    __gateway_name__ = 'tdmi'

    # Some Manifold objects doesn't exactly match with the corresponding
    # table in PostgreSQL database (or do not even exist in pgsql). 
    # Those objects are managed thanks to dedicated python objects 
    # (see for example manifold/gateways/tdmi/methods/*.py).
    # Example:
    # - Agent object provide an additionnal platform field
    # - Traceroute object crafts a SQL query involving a stored procedure.
    # - Hops does not exists in the pgsql schema and is only declared to describe
    # the type hops involved in Traceroute, we ignore queries related to hops. 

    from manifold.gateways.tdmi.methods   import Traceroute
    from manifold.gateways.tdmi.methods   import Agent 

    METHOD_MAP = {
        "traceroute" : Traceroute,   # See manifold/gateways/tdmi/methods/traceroute.py
        "agent"      : Agent,        # See manifold/gateways/tdmi/methods/agent.py
        "hop"        : None          # This is a dummy object, see metadata/tdmi.h 
    }

    def __init__(self, router, platform, platform_config):
        """
        Constructor of TDMIGateway.
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            platform_config: A dictionnary containing information to connect to the postgresql server
                Example :
                    platform_config = {
                        "db_password" : None,
                        "db_name"     : "tophat",
                        "db_user"     : "postgres",
                        "db_host"     : "clitos.ipv6.lip6.fr",
                        "db_port"     : 5432
                    }
        """

        # Every tables are private and not exposed to Manifold...
        re_ignored_tables = PostgreSQLGateway.ANY_TABLE 

        # ... excepted the following ones: 
        re_allowed_tables = [
            re.compile("^agent$"),
            re.compile("^destination$"),
            re.compile("^ip$"),
            re.compile("^node$")
        ]

        # Note: We inject some additional Manifold objects thanks to the TDMI header
        # (see /usr/share/manifold/metadata/tdmi.h). We need to load them in
        # order to support queries involving the traceroute table in a JOIN.

        # Some Fields do not exists in TDMI's database but are exposed to Manifold
        # (see /usr/share/manifold/metadata/tdmi.h) so we inject the missing Fields
        # in order to get a class consistent with the underlying ontology.
        self.custom_fields = {
            "agent" : [
                Field("string", "platform", ["const"], None, "Platform annotation, always equal to 'tdmi'")
            ]
        }

        # The following keys are not deduced from the PostgreSQL schema, so we
        # inject them manually since they will be needed to perform joins
        # among the TDMI's tables
        Log.tmp("Setting custom_keys")
        self.custom_keys = {
            "ip" : ["ip"]
        #    "agent" : [["ip", "platform"]]
        }

        super(TDMIGateway, self).__init__(router, platform, platform_config, re_ignored_tables, re_allowed_tables,None,self.custom_keys,self.custom_fields)

    def receive_impl(self, packet): 
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        table_name = query.get_table_name()

        if table_name in TDMIGateway.METHOD_MAP.keys():
            if TDMIGateway.METHOD_MAP[table_name]:
                if not query.get_action() == "get":
                    raise RuntimeError("Invalid action (%s) on '%s::%s' table" % (query.get_action(), self.get_platform_name(), table_name))

                # See manifold/gateways/tdmi/methods/*
                instance = TDMIGateway.METHOD_MAP[table_name](query, db = self)
                sql = instance.get_sql()
                rows = self.selectall(sql, None)

                if instance.need_repack and instance.repack:
                    # Does this object tweak the Record returned by selectall?
                    if instance.need_repack(query):
                        rows = [instance.repack(query, row) for row in rows]
            else:
                # Dummy object, like hops (hops is declared in tdmi.h) but
                # do not corresponds to any table in the TDMI database 
                Log.warning("TDMI::forward(): Querying a dummy object (%s)" % table_name)
                rows = list()

            self.records(rows, packet)
            
        else:
            # Update FROM clause according to postgresql aliases
            query.object = self.get_pgsql_name(table_name)
            super(TDMIGateway, self).receive_impl(packet)

