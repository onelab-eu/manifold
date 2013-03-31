
# COMMENT ON TABLE mytable IS 'This is my table.';
#
# All comments are stored in pg_description
# To get the comments on a table, you need to join it to pg_class
#
# SELECT obj_description(oid)
# FROM pg_class
# WHERE relkind = 'r'
#
#
# COMMENT ON COLUMN addresses.address_id IS 'Unique identifier for the> addresses table';

# Some code borrowed from MyPLC PostgreSQL code
import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
# UNICODEARRAY not exported yet
psycopg2.extensions.register_type(psycopg2._psycopg.UNICODEARRAY)

import re, datetime
import pgdb
from uuid                               import uuid4
from types                              import StringTypes, NoneType
from pprint                             import pformat
from manifold.gateways                  import Gateway
from manifold.util.log                  import *
from manifold.core.capabilities         import Capabilities
from manifold.util.predicate            import and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg, contains
from manifold.core.table                import Table
from manifold.core.field                import Field 
from manifold.core.announce             import Announce


class PostgreSQLGateway(Gateway):

    SQL_STR = "SELECT %(fields)s FROM %(table)s WHERE %(filters)s";

    SQL_OPERATORS = {
        eq: '='
    }

    #-------------------------------------------------------------------------------
    # METADATA 
    #-------------------------------------------------------------------------------

    SQL_DATABASE_NAMES = """
    SELECT datname FROM pg_database
    WHERE datistemplate = false;
    """

    SQL_TABLE_NAMES = """
    SELECT
        table_name 
    FROM
        information_schema.tables 
    WHERE table_schema='public' AND table_type='BASE TABLE'
    """

    SQL_TABLE_FIELDS = """
    SELECT 
        column_name, data_type, is_updatable, is_nullable
    FROM
        information_schema.columns
    WHERE
        table_name=%s ORDER BY ordinal_position
    """

    SQL_TABLE_KEYS = """
    SELECT
        tc.table_name,
        kcu.column_name  
    FROM   
        information_schema.table_constraints AS tc   
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
    WHERE 
        constraint_type = 'PRIMARY KEY' AND tc.table_name=%s;
    """

    SQL_TABLE_FOREIGN_KEYS = """
    SELECT
        kcu.column_name, 
        ccu.table_name AS foreign_table_name
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
    WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=%s;
    """
    # FULL REQUEST:
    # SELECT
    #     tc.constraint_name, tc.table_name, kcu.column_name, 
    #     ccu.table_name AS foreign_table_name,
    #     ccu.column_name AS foreign_column_name 
    # FROM 
    #     information_schema.table_constraints AS tc 
    #     JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
    #     JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
    # WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=%s;

    #---------------------------------------------------------------------------
    # PostgreSQL interface and helper functions
    #---------------------------------------------------------------------------

    def __init__(self, router, platform, query, config, user_config, user):
        super(PostgreSQLGateway, self).__init__(router, platform, query, config, user_config, user)
        self.debug = False
        #self.debug = True
        self.connection = None

    def cursor(self, cursor_factory=None): #psycopg2.extras.NamedTupleCursor
        if self.connection is None:
            # (Re)initialize database connection
            cfg = {
                'user':     self.config['db_user'],
                'password': self.config['db_password']
            }
            cfg['database'] = self.config['db_name'] if 'db_name' in self.config else 'postgres'

            if psycopg2:
                try:
                    # Try UNIX socket first
                    self.connection = psycopg2.connect(**cfg)
                except psycopg2.OperationalError:
                    # Fall back on TCP
                    cfg['host'] = self.config['db_host']
                    cfg['port'] = self.config['db_port'] if 'db_port' in self.config else 5432
                    self.connection = psycopg2.connect(**cfg)
                self.connection.set_client_encoding("UNICODE")
            else:
                cfg['host'] = "%s:%d" % (api.config.TOPHAT_DB_HOST, api.config.TOPHAT_DB_PORT)
                self.connection = pgdb.connect(**cfg)

        (self.rowcount, self.description, self.lastrowid) = \
                        (None, None, None)

        if cursor_factory:
            return self.connection.cursor(cursor_factory=cursor_factory)
        else:
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
    def execute(self, query, params = None, cursor_factory=None):
        cursor = self.cursor(cursor_factory)
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
                    log_debug('execute0',query)
                cursor.execute(query)
            elif isinstance(params,dict):
                if self.debug:
                    log_debug('execute-dict: params',params,'query',query%params)
                cursor.execute(query,params)
            elif isinstance(params,tuple) and len(params)==1:
                if self.debug:
                    log_debug('execute-tuple',query%params[0])
                cursor.execute(query,params[0])
            else:
                #param_seq=(params,)
                param_seq=params
                if self.debug:
                    for params in param_seq:
                        log_debug('executemany',query%params)
                cursor.executemany(query, param_seq)
            (self.rowcount, self.description, self.lastrowid) = \
                            (cursor.rowcount, cursor.description, cursor.lastrowid)
        except Exception, e:
            try:
                self.rollback()
            except:
                pass
            uuid = uuid4() #commands.getoutput("uuidgen")
            log_debug("Database error %s:" % uuid)
            log_debug(e)
            log_debug("Query:")
            log_debug(query)
            log_debug("Params:")
            log_debug(pformat(params))
            msg = str(e).rstrip() # jordan
            raise Exception("Please contact " + \
                             self.config['name'] + " Support " + \
                             "<" + self.config['mail_support_address'] + ">" + \
                             " and reference " + uuid + " - " + msg)

        return cursor

    # see instead: psycopg2.extras.NamedTupleCursor
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

        if isinstance(value, (list, tuple, set)):
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
            'table': self.query.fact_table,
            'filters': self.get_where(self.query.filters),
            'fields': ', '.join(self.query.fields)
        }
        sql = self.SQL_STR % params
        return sql

    def forward(self, query, deferred=False, execute=True, user=None):
        self.query = query
        self.start()

    def start(self):
        sql = self.get_sql()
        params = None

        for row in self.selectall(sql, params):
            self.callback(row)
        self.callback(None)

        return 

    def get_databases(self):
        return [x['datname'] for x in self.selectall(self.SQL_DATABASE_NAMES) if x['datname'] != 'postgres']

    def get_metadata(self):
        announces = []
        
        curs = self.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        curs.execute(self.SQL_TABLE_NAMES)
        tables = (x[0] for x in curs.fetchall())
        
        for table_name in tables:
        
            t = Table(None, None, table_name, None, None) #fields, primary_keys[table_name])

            # FIELDS:
            fields = set()
            curs.execute(self.SQL_TABLE_FIELDS, (table_name, ))
            for field in curs.fetchall():
                # PostgreSQL types vs base types
                t.insert_field(Field(
                    qualifier   = '' if field.is_updatable == 'YES' else 'const',
                    type        = foreign_keys[field.column_name] if field.column_name in foreign_keys else field.data_type,
                    name        = field.column_name,
                    is_array    = False, # XXX we might need some heuristics here
                    description = comments[field.column_name] if field.column_name in comments else '(null)'
                ))
        
            # PRIMARY KEYS: XXX simple key ?
            # We build a key dictionary associating each table with its primary key
            curs.execute(self.SQL_TABLE_KEYS, (table_name, ))
            fks = curs.fetchall()
            primary_keys = { fk.table_name: fk.column_name for fk in fks }
            
            for k in primary_keys[table_name]:
                t.insert_key(k)

            # FOREIGN KEYS:
            # We build a foreign_keys dictionary associating each field of
            # the table with the table it references.
            curs.execute(self.SQL_TABLE_FOREIGN_KEYS, (table_name, ))
            fks = curs.fetchall()
            foreign_keys = { fk.column_name: fk.foreign_table_name for fk in fks }
        
            # COMMENTS:
            # We build a comments dictionary associating each field of the table with
            # its comment.
            comments = {}
        
        
            # PARTITIONS:
            # TODO
        
            #mc = MetadataClass('class', table_name)
            #mc.fields = fields
            #mc.keys.append(primary_keys[table_name])
        
            cap = Capabilities()
            cap.selection = True
            cap.projection = True

            announce = Announce(t, cap)
            announces.append(announce)

        return announces
