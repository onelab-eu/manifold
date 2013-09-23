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
from manifold.core.announce             import Announces
from manifold.core.field                import Field 
from manifold.gateways.gateway          import Gateway
from manifold.gateways.postgresql       import PostgreSQLGateway
from manifold.operators                 import LAST_RECORD
from manifold.util.type                 import accepts, returns 
from manifold.util.log                  import Log

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):

    def __init__(self, router, platform, config):
        """
        Constructor of TDMIGateway
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            config: A dictionnary containing information to connect to the postgresql server
                Example :
                    config = {
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

        super(TDMIGateway, self).__init__(router, platform, config, re_ignored_tables, re_allowed_tables)

        # Some Manifold objects doesn't exactly match with the corresponding
        # table in PostgreSQL database (or even do not expist in pgsql). 
        # Those objects are managed thanks to dedicated python objects 
        # (see for example manifold/gateways/tdmi/methods/*.py).
        # Example:
        # - Agent object provide an additionnal platform field
        # - Traceroute object crafts a SQL query involving a stored procedure.
        # - Hops does not exists in the pgsql schema and is only declared to describe
        # the type hops involved in Traceroute, we ignore queries related to hops. 

        from manifold.gateways.tdmi.methods   import Traceroute
        from manifold.gateways.tdmi.methods   import Agent 

        self.METHOD_MAP = {
            "traceroute" : Traceroute,   # See manifold/gateways/tdmi/methods/traceroute.py
            "agent"      : Agent,        # See manifold/gateways/tdmi/methods/agent.py
            "hop"        : None          # This is a dummy object, see metadata/tdmi.h 
        }

        # Some Fields do not exists in TDMI's database but are exposed to Manifold
        # (see /usr/share/manifold/metadata/tdmi.h) so we inject the missing Fields
        # in order to get a class consistent with the underlying ontology.
        self.custom_fields = {
            "agent" : [
                Field(["const"], "string", "platform", None, "Platform annotation, always equal to 'tdmi'")
            ]
        }

        # The following keys are not deduced from the PostgreSQL schema, so we
        # inject them manually since they will be needed to perform joins
        # among the TDMI's tables
        self.custom_keys = {
        #    "agent" : [["ip", "platform"]]
        }

        # We inject some additional Manifold objects thanks to the TDMI header
        # (see /usr/share/manifold/metadata/tdmi.h). We need to load them in
        # order to support queries involving the traceroute table in a JOIN.
        self.get_metadata()

    def forward(self, query, callback, is_deferred = False, execute = True, user = None, format = "dict", receiver = None):
        """
        Query handler.
        Args:
            query: A Query instance, reaching this Gateway.
            callback: The function called to send this record. This callback is provided
                most of time by a From Node.
                Prototype : def callback(record)
            is_deferred: A boolean set to True if this Query is async.
            execute: A boolean set to True if the treatement requested in query
                must be run or simply ignored.
            user: The User issuing the Query.
            format: A String specifying in which format the Records must be returned.
            receiver : The From Node running the Query or None. Its ResultValue will
                be updated once the query has terminated.
        Returns:
            forward must NOT return value otherwise we cannot use @defer.inlineCallbacks
            decorator. 
        """
        identifier = receiver.get_identifier() if receiver else None
        table_name = query.get_from()

        if table_name in self.METHOD_MAP.keys():
            Gateway.forward(self, query, callback, is_deferred, execute, user, format, receiver)
            if self.METHOD_MAP[table_name]:
                # See manifold/gateways/tdmi/methods/*
                params = None
                instance = self.METHOD_MAP[table_name](query, db = self)
                sql = instance.get_sql()
                rows = self.selectall(sql, params)

                if instance.need_repack and instance.repack:
                    # Does this object tweak the python dictionnary returned by selectall?
                    if instance.need_repack(query):
                        rows = [instance.repack(query, row) for row in rows]
            else:
                # Dummy object, like hops (hops is declared in tdmi.h) but
                # do not corresponds to any 
                Log.warning("TDMI::forward(): Querying a dummy object (%s)" % table_name)
                rows = list()

            rows.append(LAST_RECORD)

            for row in rows:
                self.send(row, callback, identifier)

            self.success(receiver, query)
            
        else:
            # Update FROM clause according to postgresql aliases
            query.object = self.get_pgsql_name(table_name)
            super(TDMIGateway, self).forward(query, callback, is_deferred, execute, user, format, receiver)
