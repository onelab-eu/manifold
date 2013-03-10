# Some code borrowed from MyPLC PostgreSQL code
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

import pgdb
from types import StringTypes, NoneType

# XXX We should be able to import all operators at once
from tophat.util.predicate import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains

class PostgreSQL(object):

    SQL_STR = "SELECT %(fields) FROM %(table) WHERE %(filters)";

    SQL_OPERATORS = {
        eq: '='
    }

    # XXX This should be set in a parent class
    def __str__(self):
        return "<PostgreSQLGateway %s>" % self.query

    def __init__(self):
        self.debug = False
        #self.debug = True
        self.connection = None

    #---------------------------------------------------------------------------
    # PostgreSQL interface and helper functions
    #---------------------------------------------------------------------------

    def cursor(self):
        if self.connection is None:
            # (Re)initialize database connection
            if psycopg2:
                try:
                    # Try UNIX socket first
                    self.connection = psycopg2.connect(user = self.api.config.TOPHAT_DB_USER,
                                                       password = self.api.config.TOPHAT_DB_PASSWORD,
                                                       database = self.api.config.TOPHAT_DB_NAME)
                except psycopg2.OperationalError:
                    # Fall back on TCP
                    self.connection = psycopg2.connect(user = self.api.config.TOPHAT_DB_USER,
                                                       password = self.api.config.TOPHAT_DB_PASSWORD,
                                                       database = self.api.config.TOPHAT_DB_NAME,
                                                       host = self.api.config.TOPHAT_DB_HOST,
                                                       port = self.api.config.TOPHAT_DB_PORT)
                self.connection.set_client_encoding("UNICODE")
            else:
                self.connection = pgdb.connect(user = self.api.config.TOPHAT_DB_USER,
                                               password = self.api.config.TOPHAT_DB_PASSWORD,
                                               host = "%s:%d" % (api.config.TOPHAT_DB_HOST, api.config.TOPHAT_DB_PORT),
                                               database = self.api.config.TOPHAT_DB_NAME)

        (self.rowcount, self.description, self.lastrowid) = \
                        (None, None, None)

        return self.connection.cursor()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    @classmethod
    def _quote(self, x):
        if isinstance(x, datetime):# DateTimeType):
            x = str(x)
        elif isinstance(x, unicode):
            x = x.encode( 'utf-8' )

        if isinstance(x, types.StringType):
            x = "'%s'" % str(x).replace("\\", "\\\\").replace("'", "''")
        elif isinstance(x, (types.IntType, types.LongType, types.FloatType)):
            pass
        elif x is None:
            x = 'NULL'
        elif isinstance(x, (types.ListType, types.TupleType)):
            x = '(%s)' % ','.join(map(lambda x: str(_quote(x)), x))
        elif hasattr(x, '__pg_repr__'):
            x = x.__pg_repr__()
        else:
            raise InterfaceError, 'do not know how to handle type %s' % type(x)

        return x

    # join insists on getting strings
    @classmethod
    def quote_string(self, value):
        return str(PostgreSQL.quote(value))

    @classmethod
    def quote(self, value):
        """
        Returns quoted version of the specified value.
        """

        # The pgdb._quote function is good enough for general SQL
        # quoting, except for array types.
        if isinstance(value, (list, tuple, set)):
            return "ARRAY[%s]" % ", ".join(map (PostgreSQL.quote_string, value))
        else:
            return _quote(value)

    @classmethod
    def param(self, name, value):
        # None is converted to the unquoted string NULL
        if isinstance(value, NoneType):
            conversion = "s"
        # True and False are also converted to unquoted strings
        elif isinstance(value, bool):
            conversion = "s"
        elif isinstance(value, float):
            conversion = "f"
        elif not isinstance(value, StringTypes):
            conversion = "d"
        else:
            conversion = "s"

        return '%(' + name + ')' + conversion

    def begin_work(self):
        # Implicit in pgdb.connect()
        pass

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def do(self, query, params = None):
        cursor = self.execute(query, params)
        cursor.close()
        return self.rowcount

    def next_id(self, table_name, primary_key):
        sequence = "%(table_name)s_%(primary_key)s_seq" % locals()  
        sql = "SELECT nextval('%(sequence)s')" % locals()
        rows = self.selectall(sql, hashref = False)
        if rows: 
            return rows[0][0]
            
        return None 

    def last_insert_id(self, table_name, primary_key):
        if isinstance(self.lastrowid, int):
            sql = "SELECT %s FROM %s WHERE oid = %d" % \
                  (primary_key, table_name, self.lastrowid)
            rows = self.selectall(sql, hashref = False)
            if rows:
                return rows[0][0]

        return None

    # modified for psycopg2-2.0.7 
    # executemany is undefined for SELECT's
    # see http://www.python.org/dev/peps/pep-0249/
    # accepts either None, a single dict, a tuple of single dict - in which case it execute's
    # or a tuple of several dicts, in which case it executemany's
    def execute(self, query, params = None):
        cursor = self.cursor()
        try:

            # psycopg2 requires %()s format for all parameters,
            # regardless of type.
            # this needs to be done carefully though as with pattern-based filters
            # we might have percents embedded in the query
            # so e.g. GetPersons({'email':'*fake*'}) was resulting in .. LIKE '%sake%'
            if psycopg2:
                query = re.sub(r'(%\([^)]*\)|%)[df]', r'\1s', query)
            # rewrite wildcards set by Filter.py as '***' into '%'
            query = query.replace ('***','%')

            if not params:
                if self.debug:
                    print >> log,'execute0',query
                cursor.execute(query)
            elif isinstance(params,dict):
                if self.debug:
                    print >> log,'execute-dict: params',params,'query',query%params
                cursor.execute(query,params)
            elif isinstance(params,tuple) and len(params)==1:
                if self.debug:
                    print >> log,'execute-tuple',query%params[0]
                cursor.execute(query,params[0])
            else:
                #param_seq=(params,)
                param_seq=params
                if self.debug:
                    for params in param_seq:
                        print >> log,'executemany',query%params
                cursor.executemany(query, param_seq)
            (self.rowcount, self.description, self.lastrowid) = \
                            (cursor.rowcount, cursor.description, cursor.lastrowid)
        except Exception, e:
            try:
                self.rollback()
            except:
                pass
            uuid = commands.getoutput("uuidgen")
            print >> log, "Database error %s:" % uuid
            print >> log, e
            print >> log, "Query:"
            print >> log, query
            print >> log, "Params:"
            print >> log, pformat(params)
            msg = str(e).rstrip() # jordan
            raise TopHatDBError("Please contact " + \
                             self.api.config.TOPHAT_NAME + " Support " + \
                             "<" + self.api.config.TOPHAT_MAIL_SUPPORT_ADDRESS + ">" + \
                             " and reference " + uuid + " - " + msg)

        return cursor

    def selectall(self, query, params = None, hashref = True, key_field = None):
        """
        Return each row as a dictionary keyed on field name (like DBI
        selectrow_hashref()). If key_field is specified, return rows
        as a dictionary keyed on the specified field (like DBI
        selectall_hashref()).

        If params is specified, the specified parameters will be bound
        to the query.
        """

        cursor = self.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        self.commit()
        if hashref or key_field is not None:
            # Return each row as a dictionary keyed on field name
            # (like DBI selectrow_hashref()).
            labels = [column[0] for column in self.description]
            rows = [dict(zip(labels, row)) for row in rows]

        if key_field is not None and key_field in labels:
            # Return rows as a dictionary keyed on the specified field
            # (like DBI selectall_hashref()).
            return dict([(row[key_field], row) for row in rows])
        else:
            return rows

    def fields(self, table, notnull = None, hasdef = None):
        """
        Return the names of the fields of the specified table.
        """

        if hasattr(self, 'fields_cache'):
            if self.fields_cache.has_key((table, notnull, hasdef)):
                return self.fields_cache[(table, notnull, hasdef)]
        else:
            self.fields_cache = {}

        sql = "SELECT attname FROM pg_attribute, pg_class" \
              " WHERE pg_class.oid = attrelid" \
              " AND attnum > 0 AND relname = %(table)s"

        if notnull is not None:
            sql += " AND attnotnull is %(notnull)s"

        if hasdef is not None:
            sql += " AND atthasdef is %(hasdef)s"

        rows = self.selectall(sql, locals(), hashref = False)

        self.fields_cache[(table, notnull, hasdef)] = [row[0] for row in rows]

        return self.fields_cache[(table, notnull, hasdef)]

    #---------------------------------------------------------------------------
    # Query to SQL 
    #---------------------------------------------------------------------------

    def get_where_elt(self, predicate):
        # NOTE : & | operator on list, tuple, set: if not make one
        # NOTE : in MyPLC we could have several modifiers on a field

        field, op_, value = predicate.get_tuple()
        op = None

        if instance(value, (list, tuple, set)):
            # handling filters like '~slice_id':[]
            # this should return true, as it's the opposite of 'slice_id':[] which is false
            # prior to this fix, 'slice_id':[] would have returned ``slice_id IN (NULL) '' which is unknown 
            # so it worked by coincidence, but the negation '~slice_ids':[] would return false too
            if not value:
                if op_ in [and_, or_]:
                    operator = eq
                    value = "'{}'"
                else:
                    field=""
                    operator=""
                    value = "FALSE"
            else:
                value = map(str, map(self.quote, value))
                if op_ == and_:
                    op = "@>"
                    value = "ARRAY[%s]" % ", ".join(value)
                elif op == or_:
                    op = "&&"
                    value = "ARRAY[%s]" % ", ".join(value)
                else:
                    op = "IN"
                    value = "(%s)" % ", ".join(value)
        else:
            if value is None:
                op = "IS"
                value = "NULL"
            elif isinstance(value, StringTypes) and \
                    (value.find("*") > -1 or value.find("%") > -1):
                op = "LIKE"
                # insert *** in pattern instead of either * or %
                # we dont use % as requests are likely to %-expansion later on
                # actual replacement to % done in PostgreSQL.py
                value = value.replace ('*','***')
                value = value.replace ('%','***')
                value = str(self.quote(value))
            else:
                op = "="
                if op_ == lt:
                    op = '<'
                if op_ == gt:
                    op = '>'
                if op_ == le:
                    op = '<='
                if op_ == ge:
                    op = '>='
                if isinstance(value, StringTypes) and value[-2:] != "()": # XXX
                    value = str(self.quote(value))
                if isinstance(value, datetime.datetime):
                    value = str(self.quote(str(value)))
        if field:
            clause = "\"%s\" %s %s" % (field, op, value)
        else:
            clause = "%s %s %s" % (field, op, value)

        if op_ == neg:
            clause = " ( NOT %s ) " % (clause)

        return clause

    def get_where(self, filters):
        # NOTE : How to handle complex clauses
        return ' AND '.join([self.get_where_elt(pred) for pred in filters])


    def get_sql(self):
        params = {
            'table': query.fact_table,
            'filters': query.filters,
            'fields': query.fields
        }
        sql = self.SQL_STR % params
        return sql

    def start(self):
        # XXX We never stop gateways even when finished. Investigate ?
        self.started = True

        sql = self.get_sql()
        params = None

        for row in self.selectall(sql, params):
            self.callback(row)
        self.callback(None)

        return 
