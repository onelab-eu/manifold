#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# .h parsing utilities 
#
# This file is part of the TopHat project
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.fr>

import os, re, functools
from types                          import StringTypes

from manifold.core.capabilities     import Capabilities
from manifold.core.field            import Field 
from manifold.core.key              import Key, Keys
from manifold.core.table            import Table
from manifold.util.clause           import Clause
from manifold.util.log              import Log
from manifold.util.type             import returns, accepts 

STATIC_ROUTES_DIR = "/usr/share/manifold/metadata/"

#------------------------------------------------------------------
# Constants needed for .h parsing, see import_file_h(...)
#------------------------------------------------------------------

PATTERN_OPT_SPACE    = "\s*"
PATTERN_SPACE        = "\s+"
PATTERN_COMMENT      = "(((///?.*)|(/\*(\*<)?.*\*/))*)"
PATTERN_BEGIN        = ''.join(["^", PATTERN_OPT_SPACE])
PATTERN_END          = PATTERN_OPT_SPACE.join(['', PATTERN_COMMENT, "$"])
PATTERN_SYMBOL       = "([0-9a-zA-Z_]+)"
PATTERN_CLAUSE       = "([0-9a-zA-Z_&\|\"()!=<> ]+)"
PATTERN_QUALIFIERS   = "((local|const) )?(local|const)?"
PATTERN_CLASS        = "(class)"
PATTERN_ARRAY        = "(\[\])?"
PATTERN_CLASS_BEGIN  = PATTERN_SPACE.join([PATTERN_CLASS, PATTERN_SYMBOL, "{"])
PATTERN_CLASS_FIELD  = PATTERN_SPACE.join([PATTERN_QUALIFIERS, PATTERN_SYMBOL, PATTERN_OPT_SPACE.join([PATTERN_SYMBOL, PATTERN_ARRAY, ";"])])
PATTERN_CLASS_KEY    = PATTERN_OPT_SPACE.join(["KEY\((", PATTERN_SYMBOL, "(,", PATTERN_SYMBOL, ")*)\)", ";"])
PATTERN_CLASS_CAP    = PATTERN_OPT_SPACE.join(["CAPABILITY\((", PATTERN_SYMBOL, "(,", PATTERN_SYMBOL, ")*)\)", ";"])
PATTERN_CLASS_CLAUSE = PATTERN_OPT_SPACE.join(["PARTITIONBY\((", PATTERN_CLAUSE, ")\)", ";"])
PATTERN_CLASS_END    = PATTERN_OPT_SPACE.join(["}", ";"])
PATTERN_ENUM_BEGIN   = PATTERN_SPACE.join(["enum", PATTERN_SYMBOL, "{"])
PATTERN_ENUM_FIELD   = PATTERN_OPT_SPACE.join(["\"(.+)\"", ",?"])
PATTERN_ENUM_END     = PATTERN_OPT_SPACE.join(["}", ";"])

REGEXP_EMPTY_LINE    = re.compile(''.join([PATTERN_BEGIN, PATTERN_COMMENT,      PATTERN_END]))
REGEXP_CLASS_BEGIN   = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_BEGIN,  PATTERN_END]))
REGEXP_CLASS_FIELD   = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_FIELD,  PATTERN_END]))
REGEXP_CLASS_KEY     = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_KEY,    PATTERN_END]))
REGEXP_CLASS_CAP     = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_CAP,    PATTERN_END]))
REGEXP_CLASS_CLAUSE  = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_CLAUSE, PATTERN_END]))
REGEXP_CLASS_END     = re.compile(''.join([PATTERN_BEGIN, PATTERN_CLASS_END,    PATTERN_END]))
REGEXP_ENUM_BEGIN    = re.compile(''.join([PATTERN_BEGIN, PATTERN_ENUM_BEGIN,   PATTERN_END]))
REGEXP_ENUM_FIELD    = re.compile(''.join([PATTERN_BEGIN, PATTERN_ENUM_FIELD,   PATTERN_END]))
REGEXP_ENUM_END      = re.compile(''.join([PATTERN_BEGIN, PATTERN_ENUM_END,     PATTERN_END]))

# TODO replace this class by manifold.misc.enum::Enum
class MetadataEnum:
    def __init__(self, enum_name):
        """
        Constructor
        Args:
            enum_name: The name of the enum 
        """
        self.enum_name = enum_name
        self.values = list()

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The String (%r) corresponding to this MetadataEnum
        """
        return "Enum(n = %r, v = %r)\n" % (self.enum_name, self.values)

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The String (%s) corresponding to this MetadataEnum
        """
        return self.__repr__()

#------------------------------------------------------------------
# Announce
#------------------------------------------------------------------

class Announce(object):
    def __init__(self, table, cost = None):
        """
        Constructor.
        Args:
            table: A Table instance.
            cost: The cost we've to pay to query this Table.
        """
        assert isinstance(table, Table), \
            "Invalid table = %s (%s)" % (table, type(table))
        self.table = table
        self.cost = cost

    @returns(Table)
    def get_table(self):
        """
        Returns:
            The Table instance nested in this Announce.
        """
        return self.table

    @returns(int)
    def get_cost(self):
        """
        Returns:
            The cost related to this Announce.
        """
        return self.cost

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Announce.
        """
        return "<Announce %r>" % self.table

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%r' representation of this Announce.
        """
        return "Announce: %s" % self.table


    @returns(dict)
    def to_dict(self):
        """
        Returns:
            The dict representation of this Announce.
        """
        return self.table.to_dict()

class Announces(list):

    @classmethod
    @returns(list)
    def from_dot_h(self, platform_name, gateway_type):
        return self.import_file_h(STATIC_ROUTES_DIR, platform_name, gateway_type)

    @classmethod
    @returns(list)
    def import_file_h(self, directory, platform, gateway_type):
        """
        Import a .h file (see manifold.metadata/*.h)
        Args:
            directory: A String instance containing directory storing the .h files
                Example: STATIC_ROUTES_DIR = "/usr/share/manifold/metadata/"
            platform: A String instance containing the name of the platform
                Examples: "ple", "senslab", "tdmi", "omf", ...
            gateway_type: A String instance containing the type of the Gateway.
                Examples: "sfa", "xmlrpc", "maxmind", "tdmi"
        Returns:
            A list of Announce instances, each Announce embeds a Table instance.
            This list may be empty.
        """
        # Check path
        filename = os.path.join(directory, "%s.h" % gateway_type)
        if not os.path.exists(filename):
            filename = os.path.join(directory, "%s-%s.h" % (gateway_type, platform))
            if not os.path.exists(filename):
                Log.debug("Metadata file '%s' not found (platform = %r, gateway_type = %r)" % (filename, platform, gateway_type))
                return list() 

        # Read input file
        Log.debug("Platform %s: Processing %s" % (platform, filename))
        return import_file_h(filename, platform)

    @classmethod
    def get_announces(self, metadata):
        Log.warning("what about capabilities?")
        return [Announce(t) for t in metadata.get_announce_tables()]

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Announce.
        """
        return "<Announces %r>" % self

    def __str__(self):
        return "\n".join(str(x) for x in self)

@returns(Announces)
def merge_announces(announces1, announces2):
    """
    Merge two set of announces.
    Args:
        announces1: A list of Announce instance.
        announces2: A list of Announce instance.
    Returns:
        A list of Announces instances.
    """
    s1 = frozenset(announces1)
    s2 = frozenset(announces2)
    colliding_announces = s1 & s2 
    if colliding_announces:
        Log.warning("Colliding announces: {%s}" % ", ".join([announces.get_table().get_name() for announce in colliding_announces]))
    return Announces(s1 | s2)

@returns(Announces)
def make_virtual_announces(platform_name):
    """
    Craft a list of virtual Announces corresponding to virtual
    Table supposed to be supported by any Gateway (including *:object).
    Args:
        platform_name: A name of the Storage platform.
    Returns:
        The corresponding list of Announces.
    """
    @announces_from_docstring(platform_name)
    def _get_metadata_tables():
        """
        class object {
            string  table;           /**< The name of the object/table.     */
            column  columns[];       /**< The corresponding fields/columns. */
            string  capabilities[];  /**< The supported capabilities        */

            CAPABILITY(retrieve);
            KEY(table);
        }; 

        class column {
            string qualifier;
            string name;
            string type;
            string description;
            bool   is_array;

            KEY(name);
        };

        class gateway {
            string type;

            CAPABILITY(retrieve);
            KEY(type);
        };
        """
    announces = _get_metadata_tables(platform_name)
    return announces


#------------------------------------------------------------------
# .h file parsing
#------------------------------------------------------------------

@returns(tuple)
def parse_dot_h(iterable, filename = None):
    """
    Import information stored in a .h file (see manifold/metadata/*.h)
    Args:
        iterable: The file descriptor of a successfully opened file.
            You may also pass iter(string) if the content of the .h
            is stored in "string" 
        filename: The corresponding filename. It is only used to print
            user friendly message, so you may pass None.
    Returns: A tuple made of two dictionnaries (tables, enums)
        tables:
            - key: String (the name of the class)
            - data: the corresponding Table instance
        enums:
            - key: String (the name of the enum)
            - data: the corresponding MetadataEnum instance
    Raises:
        ValueError: if the input data is not well-formed.
    """
    # Parse file
    table_name = None
    cur_enum_name = None
    tables = {}
    enums   = {}
    no_line = -1
    for line in iterable:
        line = line.rstrip("\r\n")
        is_valid = True
        error_message = None
        no_line += 1
        if REGEXP_EMPTY_LINE.match(line):
            continue
        if line[0] == '#':
            continue
        if table_name: # current scope = class
            #    local const MyType my_field[]; /**< Comment */
            m = REGEXP_CLASS_FIELD.match(line)
            if m:
                qualifiers = list() 
                if m.group(2): qualifiers.append("local")
                if m.group(3): qualifiers.append("const")

                tables[table_name].insert_field(
                    Field(
                        type        =  m.group(4),
                        name        =  m.group(5),
                        qualifiers  =  qualifiers,
                        is_array    = (m.group(6) != None), 
                        description =  m.group(7).lstrip("/*< ").rstrip("*/ ")
                    )
                )
                continue

            #    KEY(my_field1, my_field2);
            m = REGEXP_CLASS_KEY.match(line)
            if m:
                key = m.group(1).split(',')
                key = [key_elt.strip() for key_elt in key]
                tables[table_name].insert_key(key)
                # XXX
                #if key not in tables[table_name].keys:
                #     tables[table_name].keys.append(key)
                continue

            #    CAPABILITY(my_field1, my_field2);
            m = REGEXP_CLASS_CAP.match(line)
            if m:
                capability = map(lambda x: x.strip(),  m.group(1).split(','))
                tables[table_name].set_capability(capability)
                continue

            #    PARTITIONBY(clause_string);
            m = REGEXP_CLASS_CLAUSE.match(line)
            if m:
                clause_string = m.group(1)
                clause = Clause(clause_string)
                tables[table_name].partitions.append(clause)
                continue

            # };
            if REGEXP_CLASS_END.match(line):
                cur_class = tables[table_name]
                if not cur_class.keys: # we must add a implicit key
                    key_name = "%s_id" % table_name
                    if key_name in cur_class.get_field_names():
                        Log.error("Trying to add implicit key %s which is already in use" % key_name)
                    Log.info("Adding implicit key %s in %s" % (key_name, table_name))
                    dummy_key_field = Field("unsigned", key_name, ["const"], False, "Dummy key");
                    cur_class.insert_field(dummy_key_field)
                    cur_class.insert_key(Key([dummy_key_field]))
                table_name = None
                continue

            # Invalid line
            is_valid = False
            error_message = "In '%s', line %r: in table '%s': invalid line: [%r] %s" % (
                filename,
                no_line,
                table_name,
                line,
                ''.join([PATTERN_BEGIN, PATTERN_CLASS_FIELD,  PATTERN_END])
            )

        elif cur_enum_name: # current scope = enum
            #    "my string value",
            m = REGEXP_ENUM_FIELD.match(line)
            if m:
                value = m.group(1)
                continue

            # };
            if REGEXP_CLASS_END.match(line):
                cur_enum_name = None
                continue

            # Invalid line
            is_valid = False
            error_message = "In '%s', line %r: in enum '%s': invalid line: [%r]" % (filename, no_line, cur_enum_name, line)

        else: # no current scope
            # class MyClass {
            m = REGEXP_CLASS_BEGIN.match(line)
            if m:
                qualifier  = m.group(1)
                table_name = m.group(2)
                tables[table_name] = Table(None, table_name, None, Keys()) # qualifier ??
                continue

            # enum MyEnum {
            m = REGEXP_ENUM_BEGIN.match(line)
            if m:
                cur_enum_name = m.group(1)
                enums[cur_enum_name] = MetadataEnum(cur_enum_name)
                continue

            # Invalid line
            is_valid = False
            error_message = "In '%s', line %r: class declaration expected: [%r]"

        if is_valid == False:
            if not error_message:
                error_message = "Invalid input file %s, line %r: [%r]" % (filename, no_line, line)
            Log.error(error_message)
            raise ValueError(error_message)

    return (tables, enums)

@returns(Announces)
def make_announces(tables, platform):
    """
    Build a list of Announces instances based on a platform name
    an its corresponding set of Tables.
    Args:
        tables: A container carrying a set of Table instances.
        platform: A String containing the platform name.
    Returns:
        The corresponding list of Announces.
    """
    announces = Announces() 
    for table in tables.values():
        table.set_partitions(platform) # XXX This is weird
        announces.append(Announce(table))
    return announces

def check_table_consistency(tables):
    """
    Check whether a set of Tables are consistent or not.
    Param:
        tables: A container storing a set of Table instances.     
    Raises:
        ValueError: if a Table is not well-formed
    """
    for table_name, table in tables.items():
        invalid_keys = table.get_invalid_keys()
        if invalid_keys:
            error_message = "In %s: in class %r: key(s) not found: %r" % (filename, table_name, invalid_keys)
            Log.error(error_message)
            raise ValueError(error_message)

    # Rq: We cannot check type consistency while a table might refer to types provided by another file.
    # Thus we can't use get_invalid_types yet

@returns(list)
def import_file_h(filename, platform):
    """
    Parse a ".h" file (see manifold/metadata) and produce
    the corresponding Announces.
    Returns:
        The corresponding list of Announce instances.
    """
    f = open(filename, 'r')
    (classes, enum) = parse_dot_h(f, filename)
    f.close()
    check_table_consistency(classes)
    return make_announces(classes, platform)
        
@returns(Announces)
def import_string_h(string, platform):
    """
    Parse a String and produce the corresponding Announces.
    Returns:
        The corresponding list of Announce instances.
    """
    # We build an iterator on the lines in the string
    def iter(string):
        prevnl = -1
        while True:
            nextnl = string.find('\n', prevnl + 1)
            if nextnl < 0: break
            yield string[prevnl + 1:nextnl]
            prevnl = nextnl
    (classes, enum) = parse_dot_h(iter(string), None)
    check_table_consistency(classes)
    return make_announces(classes, platform)

def announces_from_docstring(platform):
    """
    Prepare a decorator which allows to define .h contents in a docstring.
    This could typically be used in a get_metadata() function.
    Args:
        platform: A String instance containing the platform name.
    Returns:
        The corresponding decorator. 
    Example:
        @announces_from_docstring('my_platform')
        def get_metadata(self):
            '''
            class my_table {
                string foo;
                int    bar;
                CAPABILITY(retrieve);
                KEY(foo);
            }; 
            '''
            ...
    """
    def decorator(fn):
        @functools.wraps(fn) # We might need to write our own wrapper to properly document the function
        def new(*args, **kwargs):
            return import_string_h(fn.__doc__, platform)
        return new
    return decorator

        
#------------------------------------------------------------------
# Tests 
#------------------------------------------------------------------

if __name__ == '__main__':
    string = """
class traceroute {
    agent       agent;          /**< The measurement agent */
    destination destination;    /**< The target IP */
    hop         hops[];         /**< IP hops discovered on the measurement */
    unsigned    hop_count;      /**< Number of IP hops */
    timestamp   first;          /**< Birth date of this IP path */
    timestamp   last;           /**< Death date of this IP path */

    CAPABILITY(join, selection, projection);
#CAPABILITY(retrieve, join, selection, projection);
    KEY(agent, destination);
}; 
    """
    import pprint
    announces = import_string_h(string, 'local')
    pprint.pprint(announces)
    
