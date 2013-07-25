#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with the traceroute table 
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2012-2013 UPMC 
#

from types                          import StringTypes
from manifold.gateways.postgresql   import PostgreSQLGateway # to_sql_where()
from manifold.util.type             import accepts, returns 

def string_to_int(s):
    """
    Convert a string into an integer
    Args:
         s: The string to convert 
    Returns:
        The corresponding integer if possible, None otherwise
    """
    try:
        i = int(s)
    except:
        i = None
    return i

#-----------------------------------------------------------------------
# \class Traceroute
# Type related to a set of traceroute records
#-----------------------------------------------------------------------

class Traceroute(list):

    @staticmethod
    def repack_hops(hops_sql, selected_sub_fields):
        """
        Convert an SQL array (ip_hop_t[]) stored in a string into the corresponding
            python dictionnary.
        Args:
            hops_sql: The string corresponding to the SQL array.
            selected_sub_fields: The queried hop's fields
        Returns:
            The correspoding python dictionnary.
        """
        hops = []
        # Unpack hops: remove NULL hops (stars) and extract every tuple
        for hop in hops_sql.lstrip('"{').rstrip('}"').replace(',NULL','').rsplit('","'):
            ip_hop = hop.strip('"').rstrip(")").lstrip("(").split(",")
            new_ip_hop = {}
            if not selected_sub_fields or "ip" in selected_sub_fields:
                new_ip_hop["ip"] = ip_hop[0]
            if not selected_sub_fields or "ttl" in selected_sub_fields: 
                new_ip_hop["ttl"] = string_to_int(ip_hop[1])
            if not selected_sub_fields or "hop_probecount" in selected_sub_fields: 
                new_ip_hop["hop_probecount"] = string_to_int(ip_hop[2])
            if not selected_sub_fields or "path" in selected_sub_fields: 
                new_ip_hop["path"] = string_to_int(ip_hop[3])
            hops.append(new_ip_hop)
        return hops

    def register_field(self, field_name):
        """
        Register a field in SELECT
        Args:
            field_name The field name
        """
        if field_name not in self.selected_fields:
            field_type = "ip_hop_t[]" if field_name == "hops" else self.map_field_type[field_name]
            self.table_fields_sql.append("%(field_name)s %(field_type)s" % {
                "field_name" : field_name,
                "field_type" : field_type 
            })
            self.selected_fields.append(field_name)

    def init_select(self, select):
        """
        Internal usage. Initialize
            self.map_field_type
            self.table_fields_sql
            self.selected_fields
            self.selected_sub_fields
        Args:
            select: A list of field name corresponding to the SELECT clause in the user's query.
        Raises:
            ValueError: if parameter select is not valid
        """
        # List available fields by querying the database
        self.map_field_type = dict() 
        for record in self.db.selectall("SELECT field_name, field_type FROM get_fields('view_traceroute')", None):
            self.map_field_type[record["field_name"]] = record["field_type"]

        # Parse SELECT
        self.table_fields_sql = list()
        self.selected_fields = list()
        self.selected_sub_fields = dict() 
        if select == None:
            # By default, return every fields
            for (field_name, field_type) in self.map_field_type.items():
                self.register_field(field_name)
        else:
            for field_name in select:
                if field_name == "agent":
                    self.register_field("agent_id")
                elif field_name == "destination":
                    self.register_field("destination_id")
                elif self.map_field_type.has_key(field_name):
                    # This field can be deduced from postgresql
                    self.register_field(field_name)
                else:
                    # Is it a sub-field like "hops.ip" ?
                    field_split = field_name.split(".")
                    if len(field_split) == 2:
                        field_name = field_split[0]
                        sub_field_name = field_split[1]

                        # Add the related field to SELECT (e.g. "hops")
                        if (self.map_field_type.has_key(field_name)) and (field_name not in self.selected_fields):
                            self.register_field(field_name)

                        # Add the related sub-field (e.g. "ttl" or "ip") 
                        if not self.selected_sub_fields.has_key(field_name):
                            self.selected_sub_fields[field_name] = []
                        self.selected_sub_fields[field_name].append(sub_field_name)
                    else:
                        print "Ignoring invalid field '%s' in select" % field_name

            # Do we select at least one field ?
            if not self.selected_fields:
                raise ValueError("""
                    %(select)s does not select any valid fields.
                    Allowed values are: %(map_field_type)s
                    """ % {
                        "select"         : select,
                        "map_field_type" : self.map_field_type.keys()
                    })

    def init_where(self, predicates):
        """
        Internal usage. Initialize self.where_predicates
        Args:
            predicates: A list of Predicates corresponding to the where clause
                (predicates are connected thanks to a AND operator) 
        """
        for predicate in predicates:
            key = predicate.get_key()
            if isinstance(key, StringTypes):
                if key in ['agent', 'destination']:
                    key = '%s_id' % key
            else:
                key = map(lambda x: '%s_id' % x if x in ['agent', 'destination'] else x, key)
            predicate.set_key(key)
        self.where = PostgreSQLGateway.to_sql_where(predicates)

    def init_ts(self, ts):
        """
        Internal usage. Initialize self.ts_min and self.ts_max
        Args:
            ts: A StringType or a date or datetime instance containing a timestamp.
                String instances containing a timestamp must respect the following
                format:
                    '%Y-%m-%d %H:%M:%S'
                You may also pass "latest" or None which means "now"
        """
        (self.ts_min, self.ts_max) = PostgreSQLGateway.get_ts_bounds(ts)

    @returns(bool)
    def need_repack(self, query):
        """
        Test whether the fetched dictionnaries require additional treatments to
        be Manifold compliant/constitent.
        Arg:
            query: The Query instance handled by the TDMIGateway 
        """
        return (frozenset(["agent", "hops", "destination"]) & query.get_select()) != frozenset()

    @staticmethod
    def rename_field(query, traceroute, sql_field_name, manifold_field_name):
        if manifold_field_name in query.get_select():
            traceroute[manifold_field_name] = traceroute[sql_field_name]
            del traceroute[sql_field_name]

    @returns(dict)
    def repack(self, query, traceroute):
        """
        Repack a Traceroute record (dict) according to the issued Query

        Args:
            query: The Query instance handled by the TDMIGateway 
            traceroute: A dictionnary corresponding to a fetched Traceroute record 
        """
        # Craft 'hops' field if queried 
        if "hops" in query.get_select():
            print "repacking hops with traceroute = %r" % traceroute
            hops_sql = traceroute["hops"]
            print "hops_sql = %r" % hops_sql
            traceroute["hops"] = Traceroute.repack_hops(hops_sql, self.selected_sub_fields)

        Traceroute.rename_field(query, traceroute, "agent_id",       "agent")
        Traceroute.rename_field(query, traceroute, "destination_id", "destination")
        return traceroute

    def __init__(self, query, db = None):
        """
        Constructor
        Args:
            query: the query to be executed.
                query.get_select():
                    An array which contains the fields we want to retrieve
                    To see which fields are available and their types, run in pgsql:
                    SELECT field_name, field_type FROM get_fields('view_traceroute')
                    The timestamp of the measurement 
                query.get_timestamp():
                    You may also pass a tuple (ts_min, ts_max) or a list [ts_min, ts_max].
                    In this case the function fetch records which were active at [t1, t2]
                    such that [t1, t2] n [ts_min, ts_max] != emptyset.
                    In this syntax, ts_min and ts_max might be equal to None if unbounded.
            db: The TDMIGateway instance receiving the Traceroute related Query.
        """
        self.db = db

        # Init other useful members
        self.init_select(query.get_select())
        self.init_where(query.get_where())
        self.init_ts(query.get_timestamp())

    def get_sql(self):
        """
        Craft the SQL query to fetch queried traceroute records.
        """
        # We call a stored procedure which craft the appropriate SQL query
        sql = """
        SELECT make_traceroute_query(
            %(select_sql)s,
            '%(where_sql)s',
            %(ts_min_sql)s,
            %(ts_max_sql)s
        )
        """ % {
            "select_sql"       : "ARRAY['%s']" % "', '".join(self.selected_fields),
            "where_sql"        : self.where if self.where != "" else "NULL", 
            "ts_min_sql"       : self.ts_min,
            "ts_max_sql"       : self.ts_max,
            "table_fields_sql" : ", ".join(self.table_fields_sql)
        }

        for record in self.db.selectall(sql):
            sql = record["make_traceroute_query"]
            continue

        return sql
