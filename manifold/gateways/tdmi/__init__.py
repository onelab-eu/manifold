# To avoid naming conflicts when importing 
from __future__                     import absolute_import

import re
from manifold.util.type             import accepts, returns 
from manifold.core.announce         import Announces
from manifold.core.field            import Field 
from manifold.gateways              import Gateway
from manifold.gateways.postgresql   import PostgreSQLGateway

# Asynchronous support
# http://initd.org/psycopg/docs/advanced.html

class TDMIGateway(PostgreSQLGateway):
    def __init__(self, router, platform, query, config, user_config, user):
        """
        Constructor of TDMIGateway
        Args:
            router: None or a Router instance
            platform: A StringValue. You may pass u'dummy' for example
            query: None or a Query instance
            config: A dictionnary containing information to connect to the postgresql server
                Example :
                    config = {
                        'db_password' : None,
                        'db_name'     : 'tophat',
                        'db_user'     : 'postgres',
                        'db_host'     : 'clitos.ipv6.lip6.fr',
                        'db_port'     : 5432
                    }
            user_config: An empty dictionnary
            user: None or a User instance
        """
        # Every tables are private and not exposed to Manifold
        re_ignored_tables = [re.compile(".*")]

        # ... excepted "agent" and "destination".
        # TODO we should use view_agent etc.
        re_allowed_tables = [
            re.compile("^view_agent$"),
            re.compile("^view_destination$")
        ]

        super(TDMIGateway, self).__init__(router, platform, query, config, user_config, user, re_ignored_tables, re_allowed_tables)
        from manifold.gateways.tdmi.methods   import Traceroute

        # Some table doesn't exists in the PostgreSQL database.
        # Those pseudo-tables are managed by dedicated python objects 
        # (see for example manifold/gateways/tdmi/methods/*.py).
        # For instance, Traceroute object craft a SQL query involving a stored procedure.
        self.connection = None
        self.METHOD_MAP = {
            "traceroute" : Traceroute
        }

        # Some Fields do not exists in TDMI's database but are exposed to Manifold
        # (see /usr/share/manifold/metadata/tdmi.h) so we inject the missing Fields
        # in order to get a class consistent with the underlying ontology.
        self.custom_fields = {
            "agent" : [
                Field("const", "string", "platform", None, "Platform annotation, always equal to 'tdmi'")
            ]
        }

        # The following keys are not deduced from the PostgreSQL schema, so we
        # inject them manually since they will be needed to perform joins
        # among the TDMI's tables
        self.custom_keys = {
            "agent" : [["ip", "platform"]]
        }

        # Map each Manifold name to the corresponding pgsql table/view name
        self.aliases = dict() 

        # Those table are injected in manifold thanks to a dedicated metadata file
        # (here /usr/share/manifold/metadata/tdmi.h). We need to load them in
        # order to support queries involving the traceroute table in a JOIN.
        self.get_metadata()

    def get_pgsql_name(self, manifold_name):
        """
        Translate a name of Manifold object into the appropriate view/table name
        Args:
            manifold_name: the Manifold object name (for example "agent") (String instance)
        Returns:
            The corresponding pgsql name (for instance "view_agent") (String instance)
        """
        if manifold_name in self.aliases.keys():
            return self.aliases[manifold_name]
        return manifold_name

    @returns(list)
    def get_metadata(self):
        """
        Retrieve metadata related to the TDMIGateway
        """
        # Fetch metadata from pgsql views. We then remove the "view_" prefix, so
        # at least, the "foo" class is mapped to the "view_foo" pgsql view.
        announces_pgsql = super(TDMIGateway, self).get_metadata_from_names(self.get_view_names())
        re_view_name = re.compile("view_(.*)")
        for announce in announces_pgsql:
            table = announce.table
            pgsql_name = table.get_name()
            m = re_view_name.match(pgsql_name)
            if m:
                table.name = m.group(1)
                self.aliases[table.get_name()] = pgsql_name

        # Fetch metadata from .h files
        announces_h = Announces.from_dot_h(self.get_platform(), self.get_gateway_type())

        # Merge metadata (TODO check colliding names)
        announces = announces_pgsql + announces_h

        # Inject custom keys and fields
        for announce in announces:
            table = announce.table
            table_name = table.get_name()

            # Inject custom fields in their corresponding announce
            if table_name in self.custom_fields.keys():
                for field in self.custom_fields[table_name]:
                    table.insert_field(field)

            # Inject custom keys in their corresponding announce
            if table_name in self.custom_keys.keys():
                for key in self.custom_keys[table_name]:
                    table.insert_key(key)

        # TODO remove *_id fields
        return announces

    def start(self):
        """
        Translate self.query into the corresponding SQL command.
        The PostgreSQL's get_sql method is overloaded in order to redirect
        handle queries related to pseudo tables (traceroute, bgp, ...) and craft a
        customized query.
        """
        query = self.query
        print "-" * 80
        print "%s" % query
        print "%s" % query.timestamp
        print "-" * 80
        if query.object in self.METHOD_MAP.keys():
            # This object is retrieved thanks to a stored procedure
            # See manifold/gateways/tdmi/methods/*
            params = None
            obj = self.METHOD_MAP[query.object](query, db = self)
            sql = obj.get_sql()
            rows = self.selectall(sql, params)

            if obj.repack:
                rows = obj.repack(query, rows)
                
            rows.append(None)
            map(self.send, rows)
        else:
            self.query.object = self.get_pgsql_name(self.query.object)
            return super(TDMIGateway, self).start()
