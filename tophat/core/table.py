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

        # DEBUG
        if name == 'traceroute':
            print 
            print "-" * 80
            print "Table: self.name   = ", self.name 
            print "Table: self.fields = ", self.fields, type(self.fields)
            print "Table: self.keys   = ", self.keys 

    def __str__(self):
        """
        \brief Convert a Table instance into a string ('%s')
        \return The corresponding string
        """
        return "<Table name='%s' platform='%s' fields='%r' keys='%r'>" % (self.name, self.platform, self.fields, self.keys)

    def __repr__(self):
        """
        \brief Convert a Table instance into a string ('%r')
        \return The corresponding string
        """
        if self.platform:
            return "%s::%s" % (self.platform, self.name)
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
        raise ValueError("get_field: field '%s' not found in '%s'. Available fields: %s" % (field_name, self.name, self.fields))

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
            break # TODO we only consider the first key
        return fields_keys
