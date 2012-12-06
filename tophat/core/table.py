from tophat.metadata.MetadataField import MetadataField
from tophat.core.filter            import Filter
from types                         import StringTypes
from sets                          import Set

def to_frozenset(t):
    """
    \brief Convert an list of lists into a frozen set of tuples
        ex: [['a', 'b'], ['c', 'd', 'e']] becomes {('a', 'b'), ('c', 'd', 'e')}
    \param t The list of lists
    \return The corresponding frozenset of tuples
    """
    if isinstance(t, list):
        return frozenset([tuple(x) if isinstance(x, (tuple, list)) else x for x in t])
    raise TypeError("to_frozenset: invalid parameter %r (list expected)" % t)

class Table:
    """
    Implements a database table schema.
    """
    def check_fields(self, fields):
        if fields == None:
            return False
        for field in fields:
            if not isinstance(field, MetadataField):
                return False
        return True

    def check_partition(self, partition):
        if partition == None:
            return True
        return isinstance(partition, Filter)

    def check_keys(self, keys):
        if not isinstance(keys, (tuple, list, frozenset)):
            return False
        for key in keys:
            if isinstance(key, (tuple, list, frozenset)):
                for key_elt in key:
                    if not isinstance(key_elt, StringTypes):
                        return False 
            elif not isinstance(key, StringTypes):
                return False
        return True

    def __init__(self, platform, name, fields, keys, partition = None, cost = 1):
        """
        \brief Constructor
        \param platforms The name of the platforms (for example: 'ple', 'omf', ...)
            providing the data.
        \param name The name of the table (for example: 'user', ...)
        \param fields The fields involved in the table (for example 'name', 'email', ...)
        \param keys The key of the table (for example 'email')
        \param partition
        \param cost
        """
        # Check parameters
        if not self.check_fields(fields):
            raise TypeError("Table: __init__: invalid type for field '%s' (type: %r)" % (field, type(field)))
        if not self.check_partition(partition):
            raise ValueError("Table: __init__: invalid parameter partition: %s" % partition)
        if not self.check_keys(keys):
            raise TypeError("Table: __init__: invalid parameter keys: %s" % keys)

        # Initialize members
        # There will also be a list that the platform cannot provide, cf sources[i].fields
        self.platform = platform
        self.name = name
        self.fields = frozenset(fields)
        self.keys = to_frozenset(keys)
        self.partition = partition # an instance of a Filter
        self.cost = cost

    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "<{%s}::%s fields = {%s} keys = {%s}>" % (
            ', '.join([p            for p in sorted(self.platform)]),
            self.name,
            ', '.join([f.field_name for f in sorted(self.fields)]),
            self.keys
        )

    def __repr__(self):
        """
        \brief Convert a Table instance into a string ('%r')
        \return The corresponding string
        """
        if self.platform:
            return "<{%s}::%s>" % (', '.join([p for p in sorted(self.platform)]), self.name)
        else:
            return self.name

#    def get_fields_from_keys(self):
#        fields = []
#        for key in self.keys:
#            if isinstance(key, (list, tuple)):
#                fields.extend(list(key))
#            else:
#                fields.append(key)
#        return fields

    def is_key(self, key):
        """
        \brief Test whether a field is a key of this table
        \param key The name of the field.
            You might pass an tuple or a list of fields (string or MetadataField)
            if your testing a composite key.
        \return True iif only this is a key
        """
        if isinstance(key, (list)):
            key = tuple(key)
        elif isinstance(key, (StringTypes, MetadataField)):
            key = (key,)
        elif not isinstance(key, tuple):
            raise TypeError("is_key: %s must be a list, a tuple, or a string" % key)
        key = tuple([k if isinstance(k, StringTypes) else k.field_name for k in key])
        return key in self.keys

    def get_field(self, field_name):
        """
        \brief Retrieve a MetadataField instance stored in self according to
            its field name.
        \param field_name The name of the requested field.
        \return The MetadataField instance identified by 'field_name'
        """
        # Retrieve the field name
        if isinstance(field_name, MetadataField):
            field_name = field_name.field_name
        elif not isinstance(field_name, StringTypes):
            raise TypeError("get_field: '%s' has an invalid type (%s): supported types are StringTypes and MetadataField" % (field_name, type(field_name)))

        # Search the corresponding field
        for field in self.fields:
            if field.field_name == field_name:
                return field
        raise ValueError("get_field: field '%s' not found in '%s::%s'. Available fields: %s" % (field_name, self.platform, self.name, self.fields))

    def get_field_names(self):
        field_names = []
        for field in self.fields:
            field_names.append(field.field_name)
        if len(field_names) > 1:
            return tuple(field_names)
        elif len(field_names) == 1:
            return field_names[0]
        raise Exception("get_field_names: table '%s' has no field." % (self.name))

    def get_fields_from_keys(self):
        """
        \return A set of tuple of MetadataField.
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a MetadataField.
        """
        fields_keys = Set() 
        for key in self.keys:
            if isinstance(key, (tuple, list)):
                cur_key = []
                for field_name in key:
                    cur_key.append(self.get_field(field_name))
                fields_keys.add(tuple(cur_key))
            elif isinstance(key, StringTypes):
                fields_keys.add(self.get_field(key))
            elif isinstance(key, MetadataField):
                fields_keys.add(key)
            else:
                raise TypeError("Invalid key: %r (type not supported: %r)" % (key, type(key)))
        return fields_keys

    def get_names_from_keys(self):
        """
        \return A set of tuple of field names
            Each sub-array correspond to a key of 'self'.
            Each element of these subarray is a Strings
        """
        names_keys = Set() 
        for key in self.keys:
            if isinstance(key, (tuple, list)):
                name_key = []
                for field in key:
                    if isinstance(field, StringTypes):
                        name_key.append(field)
                    elif isinstance(field, MetadataField):
                        name_key.append(field.field_name)
                    else:
                        raise TypeError("Invalid field: %r (type not supported: %r)" % (field, type(field)))
                names_keys.add(tuple(name_key))
            elif isinstance(key, StringTypes):
                names_keys.add(key)
            elif isinstance(key, MetadataField):
                names_keys.add(key.field_name)
            else:
                raise TypeError("Invalid key: %r (type not supported: %r)" % (key, type(key)))
        return names_keys

    def determines(self, table):
        """
        \brief Test whether "self" determines "table" table.
            u --> v iif
                exists k | u.k == v (foreign key)
                u.p == v.p          (platform equality)
        Example: tophat::agent --> tophat::ip
        \sa tophat/util/dbgraph.py
        \param table The target candidate table
        \return True iif self --> table
        """
        if Set(self.platform) == Set(table.platform):
            keys = self.get_fields_from_keys()
            for key in keys:
                if len(list(key)) > 1:
                    continue
                else:
                    key = list(key)[0]
                if key.type == table.name:
                    return True
        return False

    def includes(self, table):
        """
        \brief Test whether "self" includes "table" table.
            u ==> v iif
                u.p <= v.p (platform inclusion)
                u.n == v.n (name equality)
                u.f <= v.f (field inclusion)
        Example: tophat::ip ==> {sonoma,tophat}::ip
        \sa tophat/util/dbgraph.py
        \param table The target candidate table
        \return True iif self ==> table
        """
        if Set(self.platform) <= Set(table.platform) and table.name == self.name: 
            fields_self  = Set([(field.field_name, field.type) for field in self.fields])
            fields_table = Set([(field.field_name, field.type) for field in table.fields])
            return fields_table <= fields_self
        return False 

#    def includes(self, table):
#        """
#        \brief Test whether "self" includes "table" table.
#           Example: "ip_hop" table includes "ip" table because one of
#             its fields is named "ip"
#        \sa tophat/util/dbgraph.py
#        \param table The target candidate table
#        \return True iif self -> table
#        """
#        return table.name in self.get_field_names() 
#
    def provides(self, table):
        """
        \brief Test whether "self" provides a "table" table.
            u ~~> f iif:
                \exists k | v.k \in u.f (foreign key)
                u.p == v.p
            Example:
                tophat::traceroute ~~> tophat::agent
        \sa tophat/util/dbgraph.py
        \param table The target candidate table
        \return True iif self ~~> table
        """
        if self.name == "traceroute" and table.name == "agent":
            print "TODO: provides(): bugged: traceroute should provides agent"
        if self.platform == table.platform:
            for key in table.keys:
                # We ignore composite key (e.g. (source, destination, ts))
                if isinstance(key, (list, tuple, set, frozenset)):
                    continue
                if isinstance(key, MetadataField):
                    key_type = key.type
                elif isinstance(key, StringTypes):
                    key_type = table.get_field(key).type
                if table.get_field(key_name).type == key_type:
                    return True
        return False


