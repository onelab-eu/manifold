# To avoid naming conflicts when importing 
from __future__                     import absolute_import

import re
from manifold.core.announce         import Announces
from manifold.core.field            import Field 
from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway
from manifold.core.record           import Record, Records, LastRecord
from manifold.util.type             import accepts, returns 
from manifold.util.log              import Log

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):
    __gateway_name__ = 'tdmi'

    def __init__(self, router, platform, query, config, user_config, user):
        """
        Constructor of TDMIGateway
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u"dummy" for example
            query: None or a Query instance
            config: A dictionnary containing information to connect to the postgresql server
                Example :
                    config = {
                        "db_password" : None,
                        "db_name"     : "tophat",
                        "db_user"     : "postgres",
                        "db_host"     : "clitos.ipv6.lip6.fr",
                        "db_port"     : 5432
                    }
            user_config: An empty dictionnary
            user: None or a User instance
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

        super(TDMIGateway, self).__init__(router, platform, query, config, user_config, user, re_ignored_tables, re_allowed_tables)

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
            "hops"       : None          # This is a dummy object, see metadata/tdmi.h 
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

    def start(self):
        """
        Translate self.get_query() into the corresponding SQL command.
        The PostgreSQL's get_sql method is overloaded in order to redirect
        handle queries related to pseudo tables (traceroute, bgp, ...) and craft a
        customized query.
        """
        query = self.get_query()
        table_name = query.get_from()

        if table_name in self.METHOD_MAP.keys():
            if self.METHOD_MAP[table_name]:
                # See manifold/gateways/tdmi/methods/*
                params = None
                instance = self.METHOD_MAP[table_name](query, db = self)
                sql = instance.get_sql()
                rows = self.selectall(sql, params)

                # Does this object tweak the python dictionnary returned by selectall?
                if instance.need_repack and instance.repack:
                    if instance.need_repack(query):
                        rows = [instance.repack(query, row) for row in rows]
            else:
                # Dummy object, like hops (hops is declared in tdmi.h) but
                # do not corresponds to any 
                rows = list()

            map(self.send, Records(rows))
            self.send(LastRecord())
        else:
            # Update FROM clause according to postgresql aliases
            self.query.object = self.get_pgsql_name(table_name)
            return super(TDMIGateway, self).start()
