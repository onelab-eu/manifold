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
#   Jordan Augé <jordan.auge@lip6.fr>

import os, re
#from manifold.metadata.MetadataClass    import MetadataClass
from manifold.core.table              import Table
from manifold.core.capabilities       import Capabilities
from manifold.core.field              import Field 
from manifold.core.key                import Key, Keys
from manifold.util.clause             import Clause
from manifold.util.type               import returns, accepts 

STATIC_ROUTES_FILE = "/usr/share/manifold/metadata/"

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
PATTERN_CONST        = "(const)?"
PATTERN_CLASS        = "(onjoin|class)"
PATTERN_ARRAY        = "(\[\])?"
PATTERN_CLASS_BEGIN  = PATTERN_SPACE.join([PATTERN_CLASS, PATTERN_SYMBOL, "{"])
PATTERN_CLASS_FIELD  = PATTERN_SPACE.join([PATTERN_CONST, PATTERN_SYMBOL, PATTERN_OPT_SPACE.join([PATTERN_SYMBOL, PATTERN_ARRAY, ";"])])
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

class MetadataEnum:
    def __init__(self, enum_name):
        """
        \brief Constructor
        \param enum_name The name of the enum 
        """
        self.enum_name = enum_name
        self.values = []

    def __repr__(self):
        """
        \return the string (%r) corresponding to this MetadataEnum
        """
        return "Enum(n = %r, v = %r)\n" % (self.enum_name, self.values)

#------------------------------------------------------------------
# .h file parsing
#------------------------------------------------------------------

def import_file_h(filename):
    """
    \brief Import a .h file (see manifold.metadata/*.h)
    \param filename The path of the .h file
    \return A tuple made of two dictionnaries (classes, enums)
        classes:
            - key: String (the name of the class)
            - data: the corresponding MetadataClass instance
        enums:
            - key: String (the name of the enum)
            - data: the corresponding MetadataEnum instance
    \sa MetadataEnum.py
    \sa MetadataClass.py
    \sa Field.py
    """
    # Load file
    fp = open(filename, "r")
    lines  = fp.readlines()
    fp.close()

    # Parse file
    cur_class_name = None
    cur_enum_name = None
    classes = {}
    enums   = {}
    no_line = -1
    for line in lines:
        line = line.rstrip("\r\n")
        is_valid = True
        no_line += 1
        if REGEXP_EMPTY_LINE.match(line):
            continue
        if line[0] == '#':
            continue
        if cur_class_name: # current scope = class
            #    const MyType my_field[]; /**< Comment */
            m = REGEXP_CLASS_FIELD.match(line)
            if m:
                classes[cur_class_name].insert_field(
                    Field(
                        qualifier   = m.group(1),
                        type        = m.group(2),
                        name        = m.group(3),
                        is_array    = (m.group(4) != None), 
                        description = m.group(5).strip("/*<")
                    )
                )
                continue

            #    KEY(my_field1, my_field2);
            m = REGEXP_CLASS_KEY.match(line)
            if m:
                key = m.group(1).split(',')
                key = [key_elt.strip() for key_elt in key]
                classes[cur_class_name].insert_key(key)
                # XXX
                #if key not in classes[cur_class_name].keys:
                #     classes[cur_class_name].keys.append(key)
                continue

            #    CAPABILITY(my_field1, my_field2);
            m = REGEXP_CLASS_CAP.match(line)
            if m:
                capability = map(lambda x: x.strip(),  m.group(1).split(','))
                classes[cur_class_name].set_capability(capability)
                continue

            #    PARTITIONBY(clause_string);
            m = REGEXP_CLASS_CLAUSE.match(line)
            if m:
                clause_string = m.group(1)
                clause = Clause(clause_string)
                classes[cur_class_name].partitions.append(clause)
                continue

            # };
            if REGEXP_CLASS_END.match(line):
                cur_class = classes[cur_class_name]
                if not cur_class.keys: # we must add a implicit key
                    key_name = "%s_id" % cur_class_name
                    if key_name in cur_class.get_field_names():
                        raise ValueError("Trying to add implicit key %s which is already in use" % key_name)
                    print "I: Adding implicit key %s in %s" % (key_name, cur_class_name) 
                    dummy_key_field = Field("const", "unsigned", key_name, False, "Dummy key");
                    cur_class.insert_field(dummy_key_field)
                    cur_class.keys.add(Key([dummy_key_field]))
                cur_class_name = None
                continue

            # Invalid line
            is_valid = False
            print "In '%s', line %r: in class '%s': invalid line: [%r]" % (filename, no_line, cur_class_name, line)

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
            print "In '%s', line %r: in enum '%s': invalid line: [%r]" % (filename, no_line, cur_enum_name, line)

        else: # no current scope
            # class MyClass {
            m = REGEXP_CLASS_BEGIN.match(line)
            if m:
                qualifier      = m.group(1)
                cur_class_name = m.group(2)
                classes[cur_class_name] = Table(None, None, cur_class_name, None, Keys()) # qualifier ??
                continue

            # enum MyEnum {
            m = REGEXP_ENUM_BEGIN.match(line)
            if m:
                cur_enum_name = m.group(1)
                enums[cur_enum_name] = MetadataEnum(cur_enum_name)
                continue

            # Invalid line
            is_valid = False
            print "In '%s', line %r: class declaration expected: [%r]"

        if is_valid == False:
            raise ValueError("Invalid input file %s, line %r: [%r]" % (filename, no_line, line))

    # Check consistency
#    for cur_class_name, cur_class in classes.items():
#        invalid_keys = cur_class.get_invalid_keys()
#        if invalid_keys:
#            raise ValueError("In %s: in class %r: key(s) not found: %r" % (filename, cur_class_name, invalid_keys))

    return (classes, enums)

#------------------------------------------------------------------
# Announce
#------------------------------------------------------------------

class Announce(object):
    def __init__(self, table, cost = None):
        """
        \brief Constructor
        """
        self.table = table
        self.cost = cost

    #@returns(Table)
    def get_table(self):
        return self.table

    def get_cost(self):
        return self.cost

    def __repr__(self):
        return "<Announce %r>" % self.table

class Announces(object):

    @classmethod
    def from_dot_h(self, platform_name, gateway_type):
        print "I: Loading headers (static routes)"
        return self.import_file_h(STATIC_ROUTES_FILE, platform_name, gateway_type)

    @classmethod
    def import_file_h(self, directory, platform, gateway_type):
        """
        \brief Import a .h file (see manifold.metadata/*.h)
        \param directory The directory storing the .h files
            Example: router.conf.STATIC_ROUTES_FILE = "/usr/share/manifold/metadata/"
        \param platform The name of the platform we are configuring
            Examples: "ple", "senslab", "tophat", "omf", ...
        \param gateway_types The type of the gateway
            Examples: "SFA", "XMLRPC", "MaxMind"
            See:
                sqlite3 /var/myslice/db.sqlite
                > select gateway_type from platform;
        """
        # Check path
        filename = os.path.join(directory, "%s.h" % gateway_type)
        if not os.path.exists(filename):
            filename = os.path.join(directory, "%s-%s.h" % (gateway_type, platform))
            if not os.path.exists(filename):
                raise Exception, "Metadata file '%s' not found (platform = %r, gateway_type = %r)" % (filename, platform, gateway_type)

        # Read input file
        print "I: Platform %s: Processing %s" % (platform, filename)
        (classes, enums) = import_file_h(filename)

        # Check class consistency
        for cur_class_name, cur_class in classes.items():
            invalid_keys = cur_class.get_invalid_keys()
            if invalid_keys:
                raise ValueError("In %s: in class %r: key(s) not found: %r" % (filename, cur_class_name, invalid_keys))

        # Rq: We cannot check type consistency while a table might refer to types provided by another file.
        # Thus we can't use get_invalid_types yet

        announces = []
        for t in classes.values():
            t.set_partitions(platform) # XXX This is weird
            announces.append(Announce(t))
        return announces

    @classmethod
    def get_announces(self, metadata):
        return [Announce(t) for t in metadata.get_announce_tables()]
        
        
