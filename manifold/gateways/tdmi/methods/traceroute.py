#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions for interacting with the traceroute data in the database
#
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2012 UPMC 
#

import types

from tdmi.util.faults import TDMIInvalidArgument 

def ts_to_sql(ts):
    """
    \brief Convert a generic timestamp into a tuple a string sql compliant 
    \param ts The input timestamp. You may pass "latest" or None (this means "now")
    \return The corresponding string
    """
    try:
        if isinstance(ts, types.StringTypes):
            if ts == "latest" or ts == None:
                ret = "NULL"
            else:
                ret = "'%s'" % ts
        elif isinstance(ts, datetime.datetime):
            ret = ts.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(ts, datetime.date):
            ret = ts.strftime('%Y-%m-%d 00:00:00')
    except:
        raise TopHatInvalidArgument, "Invalid parameter: ts = %s" % ts
    return ret

def ts_to_sql_bounds(ts):
    """
    \brief Convert a generic timestamp into a tuple (ts_min, ts_max)
    \param ts The input timestamp(s). Supported types
        ts
        [ts_min, ts_max]
        (ts_min, ts_max)
    \sa ts_to_sql
    \return The corresponding (ts_min, ts_max) tuple in SQL format
    """
    try:
        if isinstance(ts, types.StringTypes):
            ts_min = ts_max = ts
        elif type(ts) is tuple:
            (ts_min, ts_max) = ts
        elif type(ts) is list:
            [ts_min, ts_max] = ts
    except:
        raise TopHatInvalidArgument, ("Invalid parameter: ts = %s" % ts)
    return (ts_to_sql(ts_min), ts_to_sql(ts_max))

def string_to_int(s):
    """
    \brief Convert a string into an integer
    \param s The string to convert 
    \return The corresponding integer if possible, None otherwise
    """
    try:
        i = int(s)
    except:
        i = None
    return i

#-----------------------------------------------------------------------
# \class Traceroutes
# Type related to a set of traceroute records
#-----------------------------------------------------------------------

class Traceroute(list):

    @staticmethod
    def repack_hops(hops_sql, selected_sub_fields):
        """
        \brief Convert an SQL array (ip_hop_t[]) into an array of dictionnary
        \param hops_sql The string corresponding to the SQL array
        """
        hops = []
        # Unpack hops: remove NULL hops (stars) and extract every tuple
        for hop in hops_sql.lstrip('"{').rstrip('}"').replace(',NULL','').rsplit('","'):
            ip_hop = hop.strip('"').rstrip(")").lstrip("(").split(",")
            new_ip_hop = {}
            if "ip" in selected_sub_fields:
                new_ip_hop["ip"] = ip_hop[0]
            if "ttl" in selected_sub_fields: 
                new_ip_hop["ttl"] = string_to_int(ip_hop[1])
            if "hop_probecount" in selected_sub_fields: 
                new_ip_hop["hop_probecount"] = string_to_int(ip_hop[2])
            if "path" in selected_sub_fields: 
                new_ip_hop["path"] = string_to_int(ip_hop[3])
            hops.append(new_ip_hop)
        return hops

    def register_field(self, field_name, field_type):
        """
        \brief Register a field in SELECT
        \param field_name The field name
        \param field_type The corresponding SQL type
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
        \brief Internal usage. Initialize
            self.map_field_type
            self.table_fields_sql
            self.selected_fields
            self.selected_sub_fields
        \param select The fields queried by the user
        """
        # List available fields by querying the database
        self.map_field_type = {}
        for record in self.db.selectall("SELECT field_name, field_type FROM get_fields('view_traceroute')", None):
            self.map_field_type[record['field_name']] = record['field_type']

        # Parse SELECT
        self.table_fields_sql = []
        self.selected_fields = []
        self.selected_sub_fields = {}
        if select == None:
            # By default, return every fields
            for field in self.map_field_type:
                self.register_field(field[0], field[1])
        else:
            for field_name in select:
                if self.map_field_type.has_key(field_name):
                    self.register_field(field_name, self.map_field_type[field_name])
                else:
                    # Is it a sub-field like "hops.ip" ?
                    field_split = field_name.split(".")
                    if len(field_split) == 2:
                        field_name = field_split[0]
                        sub_field_name = field_split[1]

                        # Add the related field to SELECT (e.g. "hops")
                        if (self.map_field_type.has_key(field_name)) and (field_name not in self.selected_fields) :
                            self.register_field(field_name, self.map_field_type[field_name])

                        # Add the related sub-field 
                        if not self.selected_sub_fields.has_key(field_name):
                            self.selected_sub_fields[field_name] = []
                        self.selected_sub_fields[field_name].append(sub_field_name)
                    else:
                        print "Ignoring invalid field in select: %s" % field_name

            # Do we select at least one field ?
            if not self.selected_fields:
                raise TopHatInvalidArgument, """
                    %(select) does not select any valid fields.
                    Allowed values are: %(map_field_type)s
                    """ % {
                        "select"         : select,
                        "map_field_type" : self.map_field_type.keys()
                    }

    def init_where(self, predicates):
        """
        \brief Internal usage. Initialize self.where_predicates
        \param select The fields queried by the user
        """
        self.where_predicates = [] 
        for p in predicates:
            field_name, field_op, field_values = p.get_str_tuple()
            if not isinstance(field_values, list):
                field_values = [field_values]
            predicates = []
            for field_value in field_values:
                predicates.append("(%(field_name)s %(operator)s ''%(field_value)s'')" % {
                    "field_name"  : field_name,
                    "operator"    : field_op,
                    "field_value" : field_value
                })
            if len(predicates) > 0:
                self.where_predicates.append(" OR ".join(predicates))

    def init_ts(self, ts):
        """
        \brief Internal usage. Initialize self.ts_min and self.ts_max
        \param select The fields queried by the user
        """
        (self.ts_min, self.ts_max) = ts_to_sql_bounds(ts)

    def repack(self):
        """
        \brief Repack SQL tuples into dictionnaries.
            Here we convert hops (which is a string containing a SQL array of tuples)
            into an array of python dictionnary 
        """
        for field_name in self.selected_sub_fields:
            if field_name == "hops":
                for traceroute in self:
                    hops = traceroute["hops"]
                    traceroute["hops"] = self.repack_hops(hops, self.selected_sub_fields["hops"]) 

    def __init__(self, query, db = None):
        """
        \brief Constructor
        \param query the query to be executed
        \param db Pass a reference to a database instance
        """
        # Additional notes:
        #
        #\param select An array which contains the fields we want to retrieve
        #    To see which fields are available and their types, run in pgsql:
        #    SELECT field_name, field_type FROM get_fields('view_traceroute')
        #\param ts The timestamp of the measurement 
        #    You may also pass a tuple (ts_min, ts_max) or a list [ts_min, ts_max].
        #    In this case the function fetch records which were active [t1, t2]
        #    such that [t1, t2] n [ts_min, ts_max] != emptyset.
        #    In this syntax, ts_min and ts_max might be equal to None if unbounded.

        self.db = db

        # Init other useful members
        self.init_ts(query.ts)
        self.init_select(query.fields)
        self.init_where(query.filters)

        # Craft the SQL query
        sql = """
            SELECT * FROM get_view_traceroutes(
                %(select_sql)s,
                '%(where_sql)s',
                %(ts_min_sql)s,
                %(ts_max_sql)s
            )
            AS traceroute(
                %(table_fields_sql)s 
            );
            """ % {
                "select_sql"       : "ARRAY['%s']" % "', '".join(self.selected_fields),
                "where_sql"        : "(%s)" % ") AND (".join(self.where_predicates) if self.where_predicates != [] else "NULL", 
                "ts_min_sql"       : self.ts_min,
                "ts_max_sql"       : self.ts_max,
                "table_fields_sql" : ', '.join(self.table_fields_sql)
            }

        # Run the SQL query
        ret = self.db.selectall(sql)
        for row in ret:
            self.append(row)

        # Process the return result to make it more user-friendly
        self.repack()
