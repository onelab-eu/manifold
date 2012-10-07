# $Id: Filter.py 15221 2009-10-05 16:18:23Z baris $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/MySlice.Filter.py $
from types import StringTypes
try:
    set
except NameError:
    from sets import Set
    set = Set

import time
import datetime # Jordan
from operator import (
    and_, or_, inv, add, mul, sub, mod, truediv, lt, le, ne, gt, ge, eq, neg
    )
from tophat.util.misc import contains
from tophat.util.faults import *
from tophat.util.parameter import Parameter, Mixed, python_type

class Predicate:

    # New modifier: { contains 
    OPERATORS = { '=': eq, '~': ne, '<': lt, '[': le, '>': gt, ']': ge, '&': and_, '|': or_, '}': contains}

    def __init__(self, key, op, value):
        self.key = key
        if op in self.OPERATORS.keys():
            self.op = self.OPERATORS[op]
        else:
            self.op = op
        self.value = value

    def __str__(self):
        return "Pred(%s,%s,%s)" % self.get_str_tuple()

    def get_tuple(self):
        return (self.key, self.op, self.value)

    def get_str_tuple(self):
        op_str = [s for s, op in self.OPERATORS.iteritems() if op == self.op]
        op_str = op_str[0]
        return (self.key, op_str, self.value,)

    def match(self, dic, ignore_missing=False):
        
        # Can we match ?
        if self.key not in dic:
            return ignore_missing

        if self.op == eq:
            if isinstance(self.value, list):
                return (dic[self.key] in self.value) # array ?
            else:
                return (dic[self.key] == self.value)
        elif self.op == ne:
            if isinstance(self.value, list):
                return (dic[self.key] not in self.value) # array ?
            else:
                return (dic[self.key] != self.value) # array ?
        elif self.op == lt:
            if isinstance(self.value, StringTypes):
                # prefix match
                return dic[self.key].startswith('%s.' % self.value)
            else:
                return (dic[self.key] < self.value)
        elif self.op == le:
            if isinstance(self.value, StringTypes):
                return dic[self.key] == self.value or dic[self.key].startswith('%s.' % self.value)
            else:
                return (dic[self.key] <= self.value)
        elif self.op == gt:
            if isinstance(self.value, StringTypes):
                # prefix match
                return self.value.startswith('%s.' % dic[self.key])
            else:
                return (dic[self.key] > self.value)
        elif self.op == ge:
            if isinstance(self.value, StringTypes):
                # prefix match
                return dic[self.key] == self.value or self.value.startswith('%s.' % dic[self.key])
            else:
                return (dic[self.key] >= self.value)
        elif self.op == and_:
            return (dic[self.key] & self.value) # array ?
        elif self.op == or_:
            return (dic[self.key] | self.value) # array ?
        elif self.op == contains:
            method, subfield = self.key.split('.', 1)
            return not not [ x for x in dic[method] if x[subfield] == self.value] 
        else:
            raise Exception, "Unexpected table format: %r", dic

    def filter(self, dic):
        """
        Filter dic according to the current predicate.
        """

        if '.' in self.key:
            # users.hrn
            method, subfield = self.key.split('.', 1)
            if not method in dic:
                return None # XXX

            if isinstance(dic[method], dict):
                # We have a 1..1 relationship: apply the same filter to the dict
                subpred = Predicate(subfield, self.op, self.value)
                match = subpred.match(dic[method])
                return dic if match else None

            elif isinstance(dic[method], (list, tuple)):
                # 1..N relationships
                match = False
                if self.op == contains:
                    return dic if self.match(dic) else None
                else:
                    subpred = Predicate(subfield, self.op, self.value)
                    dic[method] = subpred.filter(dic[method])
                    return dic
            else:
                raise Exception, "Unexpected table format: %r", dic


        else:
            # Individual field operations: this could be simplified, since we are now using operators !!
            # XXX match
            print "current predicate", self
            print "matching", dic
            print "----"
            return dic if self.match(dic) else None

class Filter(set):
    """
    A filter is a set of predicates
    """

    @staticmethod
    def from_list(l):
        f = Filter()
        try:
            for element in l:
                f.add(Predicate(*element))
        except Exception, e:
            print "Error in setting Filter from list", e
            return None
        return f
        
    @staticmethod
    def from_dict(d):
        f = Filter()
        for key, value in d.items():
            if key[0] in Predicate.OPERATORS.keys():
                f.add(Predicate(key[1:], key[0], value))
            else:
                f.add(Predicate(key, '=', value))
        return f

    def __str__(self):
        return ' && '.join([str(x) for x in self])

    def __additem__(self, value):
        if value.__class__ != Predicate:
            raise TypeError("Element of class Predicate expected, received %s" % value.__class__.__name__)
        set.__additem__(self, value)

    def keys(self):
        return set([x.key for x in self])

    def has(self, key):
        for x in self:
            if x.key == key:
                return True
        return False

    def has_op(self, key, op):
        for x in self:
            if x.key == key and x.op == op:
                return True
        return False

    def has_eq(self, key):
        return self.has_op(key, eq)

    def get_op(self, key, op):
        for x in self:
            if x.key == key and x.op == op:
                return x.value
        raise KeyError, key

    def get_eq(self, key):
        return self.get_op(key, eq)

    def get_predicates(self, key):
        ret = []
        for x in self:
            if x.key == key:
                ret.append(x)
        return ret

#    def filter(self, dic):
#        # We go through every filter sequentially
#        for predicate in self:
#            print "predicate", predicate
#            dic = predicate.filter(dic)
#        return dic

    def match(self, dic):
        for predicate in self:
            if not predicate.match(dic, ignore_missing=True):
                return False
        return True

    def filter(self, l):
        output = []
        for x in l:
            if self.match(x):
                output.append(x)
        return output

class OldFilter(Parameter, dict):
    """
    A type of parameter that represents a filter on one or more
    columns of a database table.
    Special features provide support for negation, upper and lower bounds, 
    as well as sorting and clipping.


    fields should be a dictionary of field names and types.
    As of PLCAPI-4.3-26, we provide support for filtering on
    sequence types as well, with the special '&' and '|' modifiers.
    example : fields = {'node_id': Parameter(int, "Node identifier"),
                        'hostname': Parameter(int, "Fully qualified hostname", max = 255),
                        ...}


    filter should be a dictionary of field names and values
    representing  the criteria for filtering. 
    example : filter = { 'hostname' : '*.edu' , site_id : [34,54] }
    Whether the filter represents an intersection (AND) or a union (OR) 
    of these criteria is determined by the join_with argument 
    provided to the sql method below

    Special features:

    * a field starting with '&' or '|' should refer to a sequence type
      the semantic is then that the object value (expected to be a list)
      should contain all (&) or any (|) value specified in the corresponding
      filter value. See other examples below.
    example : filter = { '|role_ids' : [ 20, 40 ] }
    example : filter = { '|roles' : ['tech', 'pi'] }
    example : filter = { '&roles' : ['admin', 'tech'] }
    example : filter = { '&roles' : 'tech' }

    * a field starting with the ~ character means negation.
    example :  filter = { '~peer_id' : None }

    * a field starting with < [  ] or > means lower than or greater than
      < > uses strict comparison
      [ ] is for using <= or >= instead
    example :  filter = { ']event_id' : 2305 }
    example :  filter = { '>time' : 1178531418 }
      in this example the integer value denotes a unix timestamp

    * if a value is a sequence type, then it should represent 
      a list of possible values for that field
    example : filter = { 'node_id' : [12,34,56] }

    * a (string) value containing either a * or a % character is
      treated as a (sql) pattern; * are replaced with % that is the
      SQL wildcard character.
    example :  filter = { 'hostname' : '*.jp' } 

    * the filter's keys starting with '-' are special and relate to sorting and clipping
    * '-SORT' : a field name, or an ordered list of field names that are used for sorting
      these fields may start with + (default) or - for denoting increasing or decreasing order
    example : filter = { '-SORT' : [ '+node_id', '-hostname' ] }
    * '-OFFSET' : the number of first rows to be ommitted
    * '-LIMIT' : the amount of rows to be returned 
    example : filter = { '-OFFSET' : 100, '-LIMIT':25}

    Here are a few realistic examples

    GetNodes ( { 'node_type' : 'regular' , 'hostname' : '*.edu' , '-SORT' : 'hostname' , '-OFFSET' : 30 , '-LIMIT' : 25 } )
      would return regular (usual) nodes matching '*.edu' in alphabetical order from 31th to 55th

    GetPersons ( { '|role_ids' : [ 20 , 40] } )
      would return all persons that have either pi (20) or tech (40) roles

    GetPersons ( { '&role_ids' : 10 } )
    GetPersons ( { '&role_ids' : 10 } )
    GetPersons ( { '|role_ids' : [ 10 ] } )
    GetPersons ( { '|role_ids' : [ 10 ] } )
      all 4 forms are equivalent and would return all admin users in the system
    """

    def __init__(self, fields = {}, filter = {}, doc = "Attribute filter"):
        # Store the filter in our dict instance
        dict.__init__(self, filter)

        # Declare ourselves as a type of parameter that can take
        # either a value or a list of values for each of the specified
        # fields.
        self.fields = dict ( [ ( field, Mixed (expected, [expected])) 
                                 for (field,expected) in fields.iteritems() ] )

        # Null filter means no filter
        Parameter.__init__(self, self.fields, doc = doc, nullok = True)

    def sql(self, api, join_with = "AND"):
        """
        Returns a SQL conditional that represents this filter.
        """

        # So that we always return something
        if join_with == "AND":
            conditionals = ["True"]
        elif join_with == "OR":
            conditionals = ["False"]
        else:
            assert join_with in ("AND", "OR")

        # init 
        sorts = []
        clips = []

        for field, value in self.iteritems():
            # handle negation, numeric comparisons
            # simple, 1-depth only mechanism

            modifiers={'~' : False, 
                       '<' : False, '>' : False,
                       '[' : False, ']' : False,
                       '-' : False,
                       '&' : False, '|' : False,
                       '{': False ,
                       }
            def check_modifiers(field):
                if field[0] in modifiers.keys():
                    modifiers[field[0]] = True
                    field = field[1:]
                    return check_modifiers(field)
                return field
            field = check_modifiers(field)

            # filter on fields
            if not modifiers['-']:
                if field not in self.fields:
                    raise PLCInvalidArgument, "Invalid filter field '%s'" % field

                # handling array fileds always as compound values
                if modifiers['&'] or modifiers['|']:
                    if not isinstance(value, (list, tuple, set)):
                        value = [value,]

                if isinstance(value, (list, tuple, set)):
                    # handling filters like '~slice_id':[]
                    # this should return true, as it's the opposite of 'slice_id':[] which is false
                    # prior to this fix, 'slice_id':[] would have returned ``slice_id IN (NULL) '' which is unknown 
                    # so it worked by coincidence, but the negation '~slice_ids':[] would return false too
                    if not value:
                        if modifiers['&'] or modifiers['|']:
                            operator = "="
                            value = "'{}'"
                        else:
                            field=""
                            operator=""
                            value = "FALSE"
                    else:
                        value = map(str, map(api.db.quote, value))
                        if modifiers['&']:
                            operator = "@>"
                            value = "ARRAY[%s]" % ", ".join(value)
                        elif modifiers['|']:
                            operator = "&&"
                            value = "ARRAY[%s]" % ", ".join(value)
                        else:
                            operator = "IN"
                            value = "(%s)" % ", ".join(value)
                else:
                    if value is None:
                        operator = "IS"
                        value = "NULL"
                    elif isinstance(value, StringTypes) and \
                            (value.find("*") > -1 or value.find("%") > -1):
                        operator = "LIKE"
                        # insert *** in pattern instead of either * or %
                        # we dont use % as requests are likely to %-expansion later on
                        # actual replacement to % done in PostgreSQL.py
                        value = value.replace ('*','***')
                        value = value.replace ('%','***')
                        value = str(api.db.quote(value))
                    else:
                        operator = "="
                        if modifiers['<']:
                            operator='<'
                        if modifiers['>']:
                            operator='>'
                        if modifiers['[']:
                            operator='<='
                        if modifiers[']']:
                            operator='>='
                        #else:
                        #    value = str(api.db.quote(value))
                        # jordan
                        if isinstance(value, StringTypes) and value[-2:] != "()": # XXX
                            value = str(api.db.quote(value))
                        if isinstance(value, datetime.datetime):
                            value = str(api.db.quote(str(value)))
 
                #if prefix: 
                #    field = "%s.%s" % (prefix,field)
                if field:
                    clause = "\"%s\" %s %s" % (field, operator, value)
                else:
                    clause = "%s %s %s" % (field, operator, value)

                if modifiers['~']:
                    clause = " ( NOT %s ) " % (clause)

                conditionals.append(clause)
            # sorting and clipping
            else:
                if field not in ('SORT','OFFSET','LIMIT'):
                    raise PLCInvalidArgument, "Invalid filter, unknown sort and clip field %r"%field
                # sorting
                if field == 'SORT':
                    if not isinstance(value,(list,tuple,set)):
                        value=[value]
                    for field in value:
                        order = 'ASC'
                        if field[0] == '+':
                            field = field[1:]
                        elif field[0] == '-':
                            field = field[1:]
                            order = 'DESC'
                        if field not in self.fields:
                            raise PLCInvalidArgument, "Invalid field %r in SORT filter"%field
                        sorts.append("%s %s"%(field,order))
                # clipping
                elif field == 'OFFSET':
                    clips.append("OFFSET %d"%value)
                # clipping continued
                elif field == 'LIMIT' :
                    clips.append("LIMIT %d"%value)

        where_part = (" %s " % join_with).join(conditionals)
        clip_part = ""
        if sorts:
            clip_part += " ORDER BY " + ",".join(sorts)
        if clips:
            clip_part += " " + " ".join(clips)
#       print 'where_part=',where_part,'clip_part',clip_part
        return (where_part,clip_part)

