# $Id: Table.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/PLC/Table.py $
from types import StringTypes, IntType, LongType
import time
import calendar
import itertools

from tophat.util.faults import *
from tophat.util.parameter import Parameter
from tophat.core.filter import Filter

class Row(dict):
    """
    Representation of a row in a database table. To use, optionally
    instantiate with a dict of values. Update as you would a
    dict. Commit to the database with sync().
    """

    # Set this to the name of the table that stores the row.
    # e.g. table_name = "nodes"
    table_name = None

    # Set this to the name of the primary key of the table. It is
    # assumed that the this key is a sequence if it is not set when
    # sync() is called.
    # e.g. primary_key="node_id"
    primary_key = None
    
    # Secondary key is used to search string values
    # We also build an index for this key
    secondary_key = None

    # Set this to the names of tables that reference this table's
    # primary key.
    join_tables = []

    # Set this to a dict of the valid fields of this object and their
    # types. Not all fields (e.g., joined fields) may be updated via
    # sync().
    fields = {}

    # view
    view_fields = {}
    subquery_fields = {}

    # The name of the view that extends objects with tags
    # e.g. view_tags_name = "view_node_tags"
    view_tags_name = None

    # Set this to the set of tags that can be returned by the Get function
    tags = {}

    def __init__(self, api, fields = {}):
        dict.__init__(self, fields)
        self.api = api
        # run the class_init initializer once
        cls=self.__class__
        if not hasattr(cls,'class_inited'):
            cls.class_init (api)
            cls.class_inited=True # actual value does not matter

    def validate(self):
        """
        Validates values. Will validate a value with a custom function
        if a function named 'validate_[key]' exists.
        """

        # Warn about mandatory fields
        mandatory_fields = self.api.db.fields(self.table_name, notnull = True, hasdef = False)
        for field in mandatory_fields:
            if not self.has_key(field) or self[field] is None:
                raise PLCInvalidArgument, field + " must be specified and cannot be unset in class %s"%self.__class__.__name__

        # Validate values before committing
        for key, value in self.iteritems():
            if value is not None and hasattr(self, 'validate_' + key):
                validate = getattr(self, 'validate_' + key)
                self[key] = validate(value)
    
    def separate_types(self, items):
        """
        Separate a list of different typed objects. 
        Return a list for each type (ints, strs and dicts)
        """
        
        if isinstance(items, (list, tuple, set)):
            ints = filter(lambda x: isinstance(x, (int, long)), items)
            strs = filter(lambda x: isinstance(x, StringTypes), items)
            dicts = filter(lambda x: isinstance(x, dict), items)
            return (ints, strs, dicts)         
        else:
            raise PLCInvalidArgument, "Can only separate list types" 
          

    def associate(self, *args):
        """
        Provides a means for high level api calls to associate objects
        using low level calls.
        """

        if len(args) < 3:
            raise PLCInvalidArgumentCount, "auth, field, value must be specified"
        elif hasattr(self, 'associate_' + args[1]):
            associate = getattr(self, 'associate_'+args[1])
            associate(*args)
        else:
            raise PLCInvalidArguemnt, "No such associate function associate_%s" % args[1]

    def validate_timestamp(self, timestamp, check_future = False):
        """
        Validates the specified GMT timestamp string (must be in
        %Y-%m-%d %H:%M:%S format) or number (seconds since UNIX epoch,
        i.e., 1970-01-01 00:00:00 GMT). If check_future is True,
        raises an exception if timestamp is not in the future. Returns
        a GMT timestamp string.
        """

        time_format = "%Y-%m-%d %H:%M:%S"

        if isinstance(timestamp, StringTypes):
            # calendar.timegm() is the inverse of time.gmtime()
            timestamp = calendar.timegm(time.strptime(timestamp, time_format))

        # Human readable timestamp string
        human = time.strftime(time_format, time.gmtime(timestamp))

        if check_future and timestamp < time.time():
            raise PLCInvalidArgument, "'%s' not in the future" % human

        return human

    def add_object(self, classobj, join_table, columns = None):
        """
        Returns a function that can be used to associate this object
        with another.
        """

        def add(self, obj, columns = None, commit = True):
            """
            Associate with the specified object.
            """

            # Various sanity checks
            assert isinstance(self, Row)
            assert self.primary_key in self
            assert join_table in self.join_tables
            assert isinstance(obj, classobj)
            assert isinstance(obj, Row)
            assert obj.primary_key in obj
            assert join_table in obj.join_tables

            # By default, just insert the primary keys of each object
            # into the join table.
            if columns is None:
                columns = {self.primary_key: self[self.primary_key],
                           obj.primary_key: obj[obj.primary_key]}

            params = []
            for name, value in columns.iteritems():
                params.append(self.api.db.param(name, value))

            self.api.db.do("INSERT INTO %s (%s) VALUES(%s)" % \
                           (join_table, ", ".join(columns), ", ".join(params)),
                           columns)

            if commit:
                self.api.db.commit()
    
        return add

    add_object = classmethod(add_object)

    def remove_object(self, classobj, join_table):
        """
        Returns a function that can be used to disassociate this
        object with another.
        """

        def remove(self, obj, commit = True):
            """
            Disassociate from the specified object.
            """
    
            assert isinstance(self, Row)
            assert self.primary_key in self
            assert join_table in self.join_tables
            assert isinstance(obj, classobj)
            assert isinstance(obj, Row)
            assert obj.primary_key in obj
            assert join_table in obj.join_tables
    
            self_id = self[self.primary_key]
            obj_id = obj[obj.primary_key]
    
            self.api.db.do("DELETE FROM %s WHERE %s = %s AND %s = %s" % \
                           (join_table,
                            self.primary_key, self.api.db.param('self_id', self_id),
                            obj.primary_key, self.api.db.param('obj_id', obj_id)),
                           locals())

            if commit:
                self.api.db.commit()

        return remove

    remove_object = classmethod(remove_object)

    # convenience: check in dict (self.fields or self.tags) that a key is writable
    @staticmethod
    def is_writable (key,value,dict):
        # if not mentioned, assume it's writable (e.g. deleted ...)
        if key not in dict: return True
        # if mentioned but not linked to a Parameter object, idem
        if not isinstance(dict[key], Parameter): return True
        # if not marked ro, it's writable
        if not dict[key].ro: return True
        return False

    def db_fields(self, obj = None):
        """
        Return only those fields that can be set or updated directly
        (i.e., those fields that are in the primary table (table_name)
        for this object, and are not marked as a read-only Parameter.
        """

        if obj is None: obj = self

        db_fields = self.api.db.fields(self.table_name)
        return dict ( [ (key,value) for (key,value) in obj.items()
                        if key in db_fields and
                        Row.is_writable(key,value,self.fields) ] )

    def tag_fields (self, obj=None):
        """
        Return the fields of obj that are mentioned in tags
        """
        if obj is None: obj=self
        
        return dict ( [ (key,value) for (key,value) in obj.iteritems() 
                        if key in self.tags and Row.is_writable(key,value,self.tags) ] )
    
    # takes as input a list of columns, sort native fields from tags
    # returns 2 dicts and one list : fields, tags, rejected
    # XXX we should not make view_fields mandatory
    @classmethod
    def parse_columns (cls, columns):
        (fields,subfields,tags,rejected, groupby)=({},{},{},[], [])

        def add_subfield(type, klass, field):
            if not type in subfields:
                subfields[type] = {}
            if not klass in subfields[type]:
                subfields[type][klass] = []
            if field:
                subfields[type][klass].append(field)

        nb_agg = 0
        for column in columns:
            if column[0] in Table.aggregates and (column[1:] in cls.view_fields or not column[1:]):
                # aggregated field
                agg = column[0]
                if not column[1:]:
                    column = Table.aggregates[agg] + '(' + cls.primary_key + ')'
                else:
                    column = Table.aggregates[agg] + '(' + column[1:] + ')'
                # WARNING: in the end, not all aggregates will be integer
                fields[column]= Parameter('int', Table.aggregates[agg])
                nb_agg += 1
            #elif column in cls.fields: fields[column]=cls.fields[column] # MySlice accommodation for views
            elif column in cls.view_fields:
                fields[column]=cls.view_fields[column] # MySlice accommodation for views
                groupby.append(column)
            elif column == 'colocated':
                # XXX default OR same values ?
                add_subfield(1, cls, cls.default_fields)
            elif column in cls.subquery_fields.keys():
                # colocated (nodes,agents)
                # XXX agents (nodes)
                klass = cls.subquery_fields[cols[0]]
                add_subfield(0, klass, klass.default_fields)
            elif column.find('.') != -1:
                # XXX we also need to treat the case "agents" which means agents.DEFAULT
                cols = column.split('.')
                if cols[0] == 'colocated':
                    # size 3 max
                    if cols[1] in cls.subquery_fields.keys():
                        klass = cls.subquery_fields[cols[1]]
                        if len(cols) > 2:
                            # colocated.agents.FIELD (node)
                            add_subfield(2, klass, cols[2])
                        else:
                            # colocated.agents
                            add_subfield(2, klass, klass.default_fields)
                        # We need to add a colocated
                        add_subfield(1, cls, None)
                    else:
                        if len(cols) > 2:
                            raise PLCInvalidArgument, "Wrong field specified"
                        # colocated.FIELD
                        add_subfield(1, cls, cols[1])
                else:
                    if len(cols) > 2:
                        raise PLCInvalidArgument, "Too many depth levels"
                    # agents.hostname
                    klass = cls.subquery_fields[cols[0]]
                    add_subfield(0, klass, cols[1])
            elif column in cls.tags: tags[column]=cls.tags[column]
            else: rejected.append(column)

        if nb_agg == 0:
            groupby = []

        return (fields,subfields,tags,rejected, groupby)

    # compute the 'accepts' part of a method, from a list of column names, and a fields dict
    # use exclude=True to exclude the column names instead
    # typically accepted_fields (Node.fields,['hostname','model',...])
    @staticmethod
    def accepted_fields (update_columns, fields_dict, exclude=False):
        result={}
        for (k,v) in fields_dict.iteritems():
            if (not exclude and k in update_columns) or (exclude and k not in update_columns):
                result[k]=v
        return result

    # filter out user-provided fields that are not part of the declared acceptance list
    # keep it separate from split_fields for simplicity
    # typically check_fields (<user_provided_dict>,{'hostname':Parameter(str,...),'model':Parameter(..)...})
    @staticmethod
    def check_fields (user_dict, accepted_fields):
# avoid the simple, but silent, version
#        return dict ([ (k,v) for (k,v) in user_dict.items() if k in accepted_fields ])
        result={}
        for (k,v) in user_dict.items():
            if k in accepted_fields: result[k]=v
            else: raise PLCInvalidArgument ('Trying to set/change unaccepted key %s'%k)
        return result

    # given a dict (typically passed to an Update method), we check and sort
    # them against a list of dicts, e.g. [Node.fields, Node.related_fields]
    # return is a list that contains n+1 dicts, last one has the rejected fields
    @staticmethod
    def split_fields (fields, dicts):
        result=[]
        for x in dicts: result.append({})
        rejected={}
        for (field,value) in fields.iteritems():
            found=False
            for i in range(len(dicts)):
                candidate_dict=dicts[i]
                if field in candidate_dict.keys():
                    result[i][field]=value
                    found=True
                    break 
            if not found: rejected[field]=value
        result.append(rejected)
        return result
        
    # split_filter # same than split_fields but it takes filter modifiers into account
    @staticmethod
    def split_filter (fields, dicts):
        result=[]
        for x in dicts: result.append({})
        rejected={}
        for (field,value) in fields.iteritems():
            found=False
            for i in range(len(dicts)):
                candidate_dict=dicts[i]
                if field in candidate_dict.keys() or (field[0] in Filter.modifiers and field[1:] in candidate_dict.keys()):
                    result[i][field]=value
                    found=True
                    break 
            if not found: rejected[field]=value
        result.append(rejected)
        return result

    ### class initialization : create tag-dependent cross view if needed
    @classmethod
    def tagvalue_view_name (cls, tagname):
        return "tagvalue_view_%s_%s"%(cls.primary_key,tagname)

    @classmethod
    def tagvalue_view_create_sql (cls,tagname):
        """
        returns a SQL sentence that creates a view named after the primary_key and tagname, 
        with 2 columns
        (*) column 1: primary_key 
        (*) column 2: actual tag value, renamed into tagname
        """

        if not cls.view_tags_name: 
            raise Exception, 'WARNING: class %s needs to set view_tags_name'%cls.__name__

        table_name=cls.table_name
        primary_key=cls.primary_key
        view_tags_name=cls.view_tags_name
        tagvalue_view_name=cls.tagvalue_view_name(tagname)
        return 'CREATE OR REPLACE VIEW %(tagvalue_view_name)s ' \
            'as SELECT %(table_name)s.%(primary_key)s,%(view_tags_name)s.value as "%(tagname)s" ' \
            'from %(table_name)s right join %(view_tags_name)s using (%(primary_key)s) ' \
            'WHERE tagname = \'%(tagname)s\';'%locals()

    @classmethod
    def class_init (cls,api):
        cls.tagvalue_views_create (api)

    @classmethod
    def tagvalue_views_create (cls,api):
        if not cls.tags: return
        for tagname in cls.tags.keys():
            api.db.do(cls.tagvalue_view_create_sql (tagname))
        api.db.commit()

    def __eq__(self, y):
        """
        Compare two objects.
        """

        # Filter out fields that cannot be set or updated directly
        # (and thus would not affect equality for the purposes of
        # deciding if we should sync() or not).
        x = self.db_fields()
        if isinstance(y, Row):
            # Jordan
            # otherwise commands like : if 'error' in r (r being a Row) fail
            y = self.db_fields(y)
        return dict.__eq__(x, y)

    def sync(self, commit = True, insert = None):
        """
        Flush changes back to the database.
        """

        # Validate all specified fields
        self.validate()

        # Filter out fields that cannot be set or updated directly
        db_fields = self.db_fields()

        # Parameterize for safety
        keys = db_fields.keys()
        values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]

        # If the primary key (usually an auto-incrementing serial
        # identifier) has not been specified, or the primary key is the
        # only field in the table, or insert has been forced.
        if not self.has_key(self.primary_key) or \
           keys == [self.primary_key] or \
           insert is True:
        
            # If primary key id is a serial int and it isnt included, get next id
            if self.fields[self.primary_key].type in (IntType, LongType) and \
               self.primary_key not in self:
                pk_id = self.api.db.next_id(self.table_name, self.primary_key)
                self[self.primary_key] = pk_id
                db_fields[self.primary_key] = pk_id
                keys = db_fields.keys()
                values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]
            # Insert new row
            sql = "INSERT INTO %s (%s) VALUES (%s)" % \
                  (self.table_name, ", ".join(keys), ", ".join(values))
        else:
            # Update existing row
            columns = ["%s = %s" % (key, value) for (key, value) in zip(keys, values)]
            sql = "UPDATE %s SET " % self.table_name + \
                  ", ".join(columns) + \
                  " WHERE %s = %s" % \
                  (self.primary_key,
                   self.api.db.param(self.primary_key, self[self.primary_key]))

        self.api.db.do(sql, db_fields)

        if commit:
            self.api.db.commit()

    def delete(self, commit = True):
        """
        Delete row from its primary table, and from any tables that
        reference it.
        """

        assert self.primary_key in self

        for table in self.join_tables + [self.table_name]:
            if isinstance(table, tuple):
                key = table[1]
                table = table[0]
            else:
                key = self.primary_key

            sql = "DELETE FROM %s WHERE %s = %s" % \
                  (table, key,
                   self.api.db.param(self.primary_key, self[self.primary_key]))

            self.api.db.do(sql, self)

        if commit:
            self.api.db.commit()

    @staticmethod
    def add_exported_fields(fields, array):
        ret = {}
        ret.update(fields)
        for a in array:
            if isinstance(a, tuple):
                klass, prefix = a 
                if prefix[-1] == '.':
                    d = klass.view_fields
                else:
                    d = Row.accepted_fields(klass.exported_fields, klass.fields)
                for k,v in d.items():
                    ret[prefix+k] = d[k]
            else:
                ret.update(Row.accepted_fields(a.exported_fields, a.fields))
        return ret

class Table(list):
    """
    Representation of row(s) in a database table.
    """

    aggregates = {
        '#': 'COUNT',
        '$': 'SUM',
        '@': 'AVG',
        '-': 'MIN',
        '+': 'MAX'
    }

    def __init__(self, api, classobj, filter = None, fields = None):
        self.api = api
        self.classobj = classobj
        self.rows = {}
        
        self.query(api, classobj, filter, fields)

    def query(self, api, classobj, input_filter, fields):

        # This map allows to find the correct class for a given subquery named as the key of the dict
        mapq = {
            'users': ('Person', 'Persons'),
            'resources': ('Resource', 'Resources'),
            'nodes': ('Resource', 'Resources'),
            'sources': ('MetadataSource', 'MetadataSources'),
            'fields': ('MetadataColumn', 'MetadataColumns'),
            'columns': ('MetadataColumn', 'MetadataColumns'),
            'methods': ('MetadataTable', 'MetadataTables'),
            'platforms': ('Platform', 'Platforms')
        }

        # subqueries
        subqueries = {}
        fields_orig = fields[:] if fields else None
       
        # If the filters are not a dict, no subquery possible
        if isinstance(input_filter, dict):
            todel = []
            # iterate though filters
            for k,v in input_filter.items():
                if '.' in k:
                    method, args = k.split('.',1)
                    # move filters/modifiers to the appropriate subquery
                    if method[0] in Filter.modifiers:
                        op = method[0]
                        method = method[1:]
                    else:
                        op = ''
                    if method not in subqueries:
                        subqueries[method] = {}
                    if 'filter' not in subqueries[method]:
                        subqueries[method]['filter'] = {}
                    subqueries[method]['filter'][op+args] = v
                    todel.append(k)
                    # TODO maybe filter need to be done a posteriori, see later comment in code

                    # we also need the field to be able to filter
                    if 'fields' not in subqueries[method]:
                        subqueries[method]['fields'] = []
                    subqueries[method]['fields'].append(args)

            for i in todel:
                del input_filter[i]

        if fields:
            todel = []
            for i in fields:
                if '.' in i:
                    method, args = i.split('.',1)
                    if method not in subqueries:
                        subqueries[method] = {}
                    if 'fields' not in subqueries[method]:
                        subqueries[method]['fields'] = []
                    subqueries[method]['fields'].append(args)
                    todel.append(i)
            for i in todel:
                fields.remove(i)

        # for each method requested, we have 
        #   subqueries[method][filter]
        #   subqueries[method][fields]

        # 1..N relations sometimes embedded into a single "table"
        # at least a key
        # example: users.hrn for slices in SFA (researcher = [users.hrn])
        # consider secondary key!

        # TODO
        # We need to consult metadata, maybe no '.' is present, to get default fields for subquery
        # Might consider different JOIN criteria (difference ?)
        
        # We need to collect the id's
        projections = []

        have_ids = True
        have_ids_name = {}
        for m in subqueries.keys():
            if m[:-1]+'_ids' not in classobj.fields.keys():
                have_ids = False
            else:
                have_ids_name[m] = m[:-1]+'_ids'
                fields.append(m[:-1]+'_ids')
                projections.append(m[:-1]+'_ids')

        #if subqueries and not have_ids and classobj.primary_key not in fields:
        #    projections.append(classobj.primary_key)
        #    fields.append(classobj.primary_key)

        # TODO
        # We might also need foreign keys (primary keys of subclasses)
        # This should be handled by the query tree

        # TODO We need to check filters and fields for query and subqueries !

        # Filters
        if fields:
            fields = '"%s"' % '" , "'.join(fields)
        else:
            fields = '*'
        sql = 'SELECT %s FROM %s WHERE True' % (fields, classobj.table_name_query)
        if input_filter:
            if isinstance(input_filter, (list, tuple, set)):
                # Separate the list into integers and strings
                filter_fields = {}
                ints = filter(lambda x: isinstance(x, (int, long)), input_filter)
                filter_fields[classobj.primary_key] = ints
                if classobj.secondary_key:
                    strs = filter(lambda x: isinstance(x, StringTypes), input_filter)
                    filter_fields[classobj.secondary_key] = strs
                sql_filter = Filter(classobj.view_fields, filter_fields)
                sql += " AND (%s) %s" % sql_filter.sql(api, "OR")
            elif isinstance(input_filter, dict):
                sql_filter = Filter(classobj.view_fields, input_filter)
                sql += " AND (%s) %s" % sql_filter.sql(api, "AND")
            elif classobj.secondary_key and isinstance (input_filter, StringTypes):
                sql_filter = Filter(classobj.fields, {classobj.secondary_key:[input_filter]})
                sql += " AND (%s) %s" % sql_filter.sql(api, "AND")
            elif isinstance (input_filter, int):
                sql_filter = Filter(classobj.view_fields, {classobj.primary_key:[input_filter]})
                sql += " AND (%s) %s" % sql_filter.sql(api, "AND")
            else:
                raise PLCInvalidArgument, "Wrong %s filter %r"%(classobj.primary_key, input_filter)

        self.selectall(sql)

        if subqueries:
            #if not have_ids:
            #    pkeys = [x[classobj.primary_key] for x in self]

            # we have the subquery + related fields
            for method, q in subqueries.items():
                # we get the list of all ids to request in this method
                search_keys = [x[have_ids_name[method]] for x in self]
                out = list(itertools.chain.from_iterable(search_keys))
                search_keys = list(set(out))

                # we get the name of the class to call this method
                fromname, importname = mapq[method]
                klasses = getattr(__import__('MySlice.%s' % fromname, globals(), locals(), importname), importname)
                klass = getattr(__import__('MySlice.%s' % fromname, globals(), locals(), fromname), fromname)

                # we limit our query to ids to be requested
                qfilter = q['filter'] if input_filter and 'filter' in q else {}
                qfilter[klass(api).primary_key] = search_keys
                # we add the subquery class primary key to requested fields (to be able to make the join)
                qfields = q['fields'] if 'fields' in q else []
                qfields.append(klass(api).primary_key)
                t = klasses(api, qfilter, qfields)
                # results by id
                t_by_id = {}
                for x in t:
                    t_by_id[x[klass(api).primary_key]] = x

                todel = []
                for x in self:
                    # we create a 'method' entry if and only if at least one requested fields belongs to it
                    keys = x[have_ids_name[method]]
                    if not keys:
                        if 'filter' in q:
                            # If we have no key and a filter => no match
                            todel.append(x)
                        else:
                            # Otherwise => match
                            if not method in x and [y for y in fields_orig if y.startswith(method + '.')]:
                                x[method] = []
                        continue
                    filtered = False
                    subfields = []
                    for key in keys: # ALL / ANY for filters on subqueries ???
                        if key in t_by_id: # subresults has not been removed by filter for example
                            dic = {}
                            for k,v in t_by_id[key].items():
                                if '%s.%s' % (method, k) in fields_orig:
                                    dic[k] = v
                            if dic:
                                subfields.append(dic)
                        else:
                            filtered = True
                    if filtered:
                        todel.append(x)
                    else:
                        if not method in x and [y for y in fields_orig if y.startswith(method + '.')]:
                            x[method] = subfields
                for x in todel:
                    self.remove(x)

        for x in self:
            for p in projections:
                del x[p]

        return self

#        # old code
#
#        if columns is None:
#            columns = classobj.fields
#            subcolumns={}
#            tag_columns={}
#            groupby = []
#        else:
#            #(columns,tag_columns,rejected, groupby) = classobj.parse_columns(columns)
#            (columns,subcolumns,tag_columns,rejected, groupby) = classobj.parse_columns(columns)
#            #if not columns and not tag_columns:
#            if not columns and not subcolumns and not tag_columns:
#                raise PLCInvalidArgument, "No valid return fields specified for class %s"%classobj.__name__
#            if rejected:
#                raise PLCInvalidArgument, "unknown column(s) specified %r in %s"%(rejected,classobj.__name__)
#
#        # We parse input_filter for fields related to subqueries or colocation requests
#        
#        filters = {}
#        # repeated
#        def add_subfield(type, klass, field):
#            if not type in subcolumns:
#                subcolumns[type] = {}
#            if not klass in subcolumns[type]:
#                subcolumns[type][klass] = []
#            if field:
#                subcolumns[type][klass].append(field)
#        def add_filter(type, klass, k, v):
#            if not type in filters:
#                filters[type] = {}
#            if not klass in filters[type]:
#                filters[type][klass] = {}
#            filters[type][klass][k] = v
#            add_subfield(type, klass, None)
#            if type == 2:
#                add_subfield(1, classobj, None)
#            
#        if isinstance(input_filter, dict):
#            for k,v in input_filter.items():
#                cols = k.split('.')
#                l = len(cols)
#                if l == 1:
#                    pass
#                elif l == 2:
#                    if cols[0] == 'colocated':
#                        add_filter(1, classobj, cols[1], v)
#                        del input_filter[k]
#                    elif cols[0] in classobj.subquery_fields.keys():
#                        add_filter(0, classobj.subquery_fields[cols[0]], cols[1], v)
#                        del input_filter[k]
#                    else:
#                        raise PLCInvalidArgument, 'Invalid filter: %s' % k
#                elif l == 3:
#                    if cols[0] == 'colocated':
#                        if cols[1] in classobj.subquery_fields.keys():
#                            add_filter(2, classobj.subquery_fields[cols[1]], cols[2], v)
#                            del input_filter[k]
#                        else:
#                            raise PLCInvalidArgument, 'Invalid filter: %s' % k
#                    else:
#                        raise PLCInvalidArgument, 'Invalid filter: %s' % k
#                else:
#                    raise PLCInvalidArgument, 'Invalid filter: too much depth'
#
#        self.columns = columns
#        self.tag_columns = tag_columns
#        # XXX DELETE
#        self.groupby = groupby
#
#        orderby = [] # TO COMPLETE
#
#        return_fields = {}
#
#        #
#        # SQL
#        #
#        query_class = [classobj]
#        
#        # We might need to prefix tables
#        prefixes = ['a'] # We can do better
#        added_pkey = []
#
#        def prefix_generator():
#            cpt = ord('a')
#            while True:
#                yield chr(cpt)
#                cpt += 1
#        prefix_gen = prefix_generator()
#        
#        def class_generator():
#            for i in [0,1,2]:
#                if i in subcolumns:
#                    for klass, fields in subcolumns[i].items():
#                        yield i, klass, fields
#
#        # We uses prefixes as soon as we have more than one table XXX
#        if subcolumns.keys():
#            parent_prefix = prefix_gen.next()
#            prefixes.append(parent_prefix)
#
#            # We might need to add to the requested fields the primary key of each
#            # table, so that we are able to group different as rows as the same
#            # object in the end. Here we memorize when we did it
#            if classobj.primary_key not in columns:
#                columns[classobj.primary_key] = classobj.view_fields[classobj.primary_key]
#                added_pkey.append('%s__%s' % (parent_prefix, classobj.primary_key))
#
#            # Prefix return_fields which are from the first table
#            farr = []
#            for k,v in columns.items():
#                farr.append("%s.%s as %s__%s" % (parent_prefix, k, parent_prefix, k))
#                # TODO We ignore v ???
#            sql_select = 'SELECT ' + ', '.join(farr)
#
#            sql_from = ' FROM %s %s' % (classobj.table_name_query, parent_prefix) # prefix first table
#
#            sql_orderby = ' ORDER BY %s.%s' % (parent_prefix, classobj.primary_key)
#        else:
#            parent_prefix = None
#
#            # We might need to add to the requested fields the primary key of each
#            # table, so that we are able to group different as rows as the same
#            # object in the end. Here we memorize when we did it
#            if classobj.primary_key and classobj.primary_key not in columns:
#                columns[classobj.primary_key] = classobj.view_fields[classobj.primary_key]
#                added_pkey.append(classobj.primary_key)
#
#            sql_select = 'SELECT ' + ', '.join(columns.keys())
#            sql_from = ' FROM %s' % classobj.table_name_query
#            if classobj.primary_key:
#                sql_orderby = ' ORDER BY %s' % classobj.primary_key
#            else:
#                sql_orderby = ''
#
#        # Filters
#        sql_filters = ''
#        if input_filter:
#            if isinstance(input_filter, (list, tuple, set)):
#                # Separate the list into integers and strings
#                filter_fields = {}
#                ints = filter(lambda x: isinstance(x, (int, long)), input_filter)
#                filter_fields[classobj.primary_key] = ints
#                if classobj.secondary_key:
#                    strs = filter(lambda x: isinstance(x, StringTypes), input_filter)
#                    filter_fields[classobj.secondary_key] = strs
#                input_filter = Filter(classobj.view_fields, filter_fields)
#                sql_filters += " AND (%s) %s" % input_filter.sql(api, "OR", parent_prefix)
#            elif isinstance(input_filter, dict):
#                input_filter = Filter(classobj.view_fields, input_filter)
#                sql_filters += " AND (%s) %s" % input_filter.sql(api, "AND", parent_prefix)
#            elif classobj.secondary_key and isinstance (input_filter, StringTypes):
#                input_filter = Filter(classobj.fields, {classobj.secondary_key:[input_filter]})
#                sql_filters += " AND (%s) %s" % input_filter.sql(api, "AND", parent_prefix)
#            elif isinstance (input_filter, int):
#                input_filter = Filter(classobj.view_fields, {classobj.primary_key:[input_filter]})
#                sql_filters += " AND (%s) %s" % input_filter.sql(api, "AND", parent_prefix)
#            else:
#                raise PLCInvalidArgument, "Wrong agent filter %r"%input_filter
#
#        sql_where = ''
#
#        # subqueries and colocated (sub) queries
#        for type, klass, fields in class_generator():
#            prefix = prefix_gen.next()
#            prefixes.append(prefix)
#
#            # Add primary keys to the select fields
#            if klass.primary_key not in fields:
#                fields.append(klass.primary_key)
#                added_pkey.append('%s__%s' % (prefix, klass.primary_key))
#
#            # Prefix all fields
#            farr = []
#            for f in fields:
#                farr.append("%s.%s AS %s__%s" % (prefix, f, prefix, f))
#            sql_select += ', ' + ', '.join(farr)
#
#            sql_from += ' LEFT JOIN %s %s' % (klass.table_name_query, prefix)
#            if type == 0 or type == 2: # sub (and colsub)
#                sql_from += ' ON %s.%s = %s.%s' % (parent_prefix, classobj.primary_key, prefix, classobj.primary_key)
#            elif type == 1: # col
#                # Ensure colocation thanks to location_id fields... 
#                # XXX we need to ensure beforewards that the class has location_id
#                sql_from += ' ON %s.location_id = %s.location_id' % (parent_prefix, prefix)
#                # ... and that the objects colocated exclude the current object (e.g. n.node_id != p.node_id)
#                sql_where += ' AND (%s.%s IS NULL OR %s.%s != %s.%s)' % (prefix, classobj.primary_key, parent_prefix, classobj.primary_key, prefix, classobj.primary_key)
#                # Restricting to NON NULL location_id speeds up the query XXX WRONG SINCE IT REMOVES SOME RESULTS
#                #sql_where += " AND NOT %s.location_id IS NULL" % prefix
#                parent_prefix = prefix
#
#            # Input filters
#            try:
#                f = Filter(klass.view_fields, filters[type][klass])
#                sql_filters += " AND (%s) %s" % f.sql(api, "AND", prefix)
#            except Exception, why:
#                pass #no filter
#
#            sql_orderby += ', %s.%s' % (prefix, klass.primary_key)
#
#        sql = sql_select + sql_from + ' WHERE True' + sql_filters + sql_where
#        
#        # XXX Test all this with aggregates (agg. only and colocation+agg)
#        # XXX we might need to add all non aggregated fields
#        if groupby:
#            sql += " GROUP BY " + ", ".join(groupby)
#        sql += sql_orderby
#
#        self.selectall(sql)
#
#        def process(outputs, prefix, classobj, rows, columns, subcolumns):
#            if columns:
#                prev_id = None
#                subrows = []
#                output = {}
#                only_dummy = True
#                for r in rows:
#                    try:
#                        if classobj.primary_key: # XXX
#                            new_id = r[classobj.primary_key]
#                        else:
#                            new_id = None
#                    except:
#                        new_id = r["%s__%s" % (prefix, classobj.primary_key)]
#                    if prev_id and prev_id != new_id:
#                        next_prefix = chr(ord(prefix)+1)
#                        process(output, next_prefix, classobj, subrows, None, subcolumns)
#                        if only_dummy and not output:
#                            break
#                        outputs.append(output)
#                        # We create a new object
#                        subrows = []
#                        output = {}
#                        only_dummy = True
#                    # We either attribute the fields to the object, or accumulate them
#                    accum = {}
#                    for f, val in r.items():
#                        if f[0] != prefix and f[1:3] == '__':
#                            accum[f] = val
#                        else:
#                            if f in added_pkey:
#                                continue
#                            only_dummy = False
#                            if not val and not new_id:
#                                continue
#                            if f[0] == prefix and f[1:3] == '__':
#                                output[f[3:]] = val
#                            else:
#                                output[f] = val
#                    subrows.append(accum)
#                    prev_id = new_id
#                # We finalize the object...
#                if not subcolumns:
#                    return # XXX
#                next_prefix = chr(ord(prefix)+1)
#                process(output, next_prefix, classobj, subrows, None, subcolumns)
#                if only_dummy and not output:
#                    pass
#                else:
#                    outputs.append(output)
#                return
#
#            if 0 in subcolumns:
#                # We only accumulate subrows for the first 0 object
#
#                for sq in subcolumns[0].keys(): # 'agents', 'hops'
#                    do_accum = True
#                    output = {}
#                    fieldname = [k for k,v in classobj.subquery_fields.items() if v == sq][0]
#                    outputs[fieldname] = []
#                    prev_id = None
#                    subrows = []
#                    only_dummy = True
#                    for r in rows:
#                        accum = {}
#                        try:
#                            new_id = r[sq.primary_key]
#                        except:
#                            new_id = r["%s__%s" % (prefix, sq.primary_key)]
#                        if prev_id and prev_id != new_id:
#                             do_accum = False
#                             if only_dummy:
#                                 break
#                             outputs[fieldname].append(output)
#                             output = {}
#                             only_dummy = True
#                        # We attribute the fields corresponding to the object and remove them
#                        for f, val in r.items():
#                            if f[0] != prefix and f[1:3] == '__':
#                                if do_accum:
#                                    accum[f] = val
#                            else:
#                                if f in added_pkey:
#                                    continue
#                                only_dummy = False
#                                if not val and not new_id:
#                                    continue
#                                if f[0] == prefix and f[1:3] == '__':
#                                    output[f[3:]] = val
#                                else:
#                                    output[f] = val
#                        if accum:
#                            subrows.append(accum)
#                        prev_id = new_id
#                    # We finalize the object
#                    if only_dummy:
#                        pass#del outputs[fieldname]
#                    else:
#                        outputs[fieldname].append(output)
#
#                    # We continue with row corresponding to the first object only
#                    prefix = chr(ord(prefix)+1)
#                    rows = subrows    
#
#            if 1 in subcolumns:
#                # recursive call, we build fake columns and subcolumns
#                if 1 in subcolumns:
#                    col = subcolumns[1]
#                subcol = {}
#                if 2 in subcolumns:
#                    subcol[0] = subcolumns[2]
#                    
#                #subcol[0] = {}
#                #for k, fields in subcolumns[1].items():
#                #   if k == classobj:
#                #      col[k] = fields
#                #   else: 
#                #      subcol[0][k] = fields
#                outputs['colocated'] = []
#                process(outputs['colocated'], prefix, classobj, rows, col, subcol)
#                if not outputs['colocated']:
#                    pass#del outputs['colocated']
#
#        
#        outputs = []
#
#        if subcolumns:
#            # We let the recursive function do the job...
#            process(outputs, 'a', classobj, self, columns, subcolumns)
#    
#            # Update the current Table object
#            del self[:]
#            self.extend(outputs)
#
#        # delete unnecessary output fields
#        # TODO: in the case of subcols
#        for x in self:
#            for y in added_pkey:
#                del x[y]
        

    def sync(self, commit = True):
        """
        Flush changes back to the database.
        """

        for row in self:
            row.sync(commit)

    def multisync(self, commit = True, insert = False): # added Jordan
        """
        Flush changes back to the database in a executemany.
        """

        # ToDo: We need to check that the exact same fields are specified
        list_db_fields = []

        # Let's assume first row is representative (we ought to check)
        repr = self[0]

        # If the primary key (usually an auto-incrementing serial
        # identifier) has not been specified, or the primary key is the
        # only field in the table, or insert has been forced.
        if not repr.has_key(repr.primary_key) or \
           keys == [repr.primary_key] or \
           insert is True:

            # We do sql once with representative
            sql = None
            # Filter out fields that cannot be set or updated directly
            db_fields = repr.db_fields()
            # Parameterize for safety
            keys = db_fields.keys()
            values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]
            # If primary key id is a serial int and it isnt included, add it!
            if repr.fields[repr.primary_key].type in (IntType, LongType) and \
               repr.primary_key not in repr:
                db_fields[repr.primary_key] = 0
                keys = db_fields.keys()
                values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]
            # Insert new row (jordan: we do sql once!)
            sql = "INSERT INTO %s (%s) VALUES (%s)" % \
                  (repr.table_name, ", ".join(keys), ", ".join(values))

            for row in self:
                # Validate all specified fields
                row.validate()

                # Filter out fields that cannot be set or updated directly
                db_fields = row.db_fields()

                # Parameterize for safety
                keys = db_fields.keys()
                values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]

                # If primary key id is a serial int and it isnt included, get next id
                if row.fields[row.primary_key].type in (IntType, LongType) and \
                   row.primary_key not in row:
                    pk_id = self.api.db.next_id(row.table_name, row.primary_key)
                    row[row.primary_key] = pk_id
                    db_fields[row.primary_key] = pk_id

                list_db_fields.append(db_fields)
        
        else: # Update
            for row in self:
                # Validate all specified fields
                row.validate()

                # Filter out fields that cannot be set or updated directly
                db_fields = self.db_fields()

                # Parameterize for safety
                keys = db_fields.keys()
                values = [self.api.db.param(key, value) for (key, value) in db_fields.items()]

                # Update existing row
                columns = ["%s = %s" % (key, value) for (key, value) in zip(keys, values)]
                sql = "UPDATE %s SET " % self.table_name + \
                      ", ".join(columns) + \
                      " WHERE %s = %s" % \
                      (self.primary_key,
                       self.api.db.param(self.primary_key, self[self.primary_key]))

                list_db_fields.append(db_fields)
        
        # Unique request that will fire executemany()
        self.api.db.do(sql, tuple(list_db_fields)) # This calls cursor.close()
        if commit:
            self.api.db.commit()

    def selectall(self, sql, params = None):
        """
        Given a list of rows from the database, fill ourselves with
        Row objects.
        """
        
        for row in self.api.db.selectall(sql, params):
            obj = self.classobj(self.api, row)
            self.append(obj)

    def dict(self, key_field = None):
        """
        Return ourself as a dict keyed on key_field.
        """

        if key_field is None:
            key_field = self.classobj.primary_key

        return dict([(obj[key_field], obj) for obj in self])
