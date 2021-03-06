#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from manifold.core.query     import Query
from manifold.core.filter    import Filter
from manifold.util.predicate import Predicate
from manifold.util.log       import Log
from manifold.util.clause    import Clause
import pyparsing as pp
import re

#DEBUG = True
DEBUG = False

def debug(args):
    if DEBUG: print(args)
 
class SQLParser(object):

    def __init__(self):
        """
        Our simple BNF:
        SELECT [fields[*] FROM table WHERE clause
        """

        integer = pp.Combine(pp.Optional(pp.oneOf("+ -")) + pp.Word(pp.nums)).setParseAction(lambda t:int(t[0]))
        floatNumber = pp.Regex(r'\d+(\.\d*)?([eE]\d+)?')
        point = pp.Literal( "." )
        e     = pp.CaselessLiteral( "E" )

        kw_select  = pp.CaselessKeyword('select')
        kw_update  = pp.CaselessKeyword('update')
        kw_insert  = pp.CaselessKeyword('insert')
        kw_delete  = pp.CaselessKeyword('delete')
        
        kw_from    = pp.CaselessKeyword('from')
        kw_into    = pp.CaselessKeyword('into')
        kw_where   = pp.CaselessKeyword('where')
        kw_at      = pp.CaselessKeyword('at')
        kw_set     = pp.CaselessKeyword('set')
        kw_true    = pp.CaselessKeyword('true').setParseAction(lambda t: 1)
        kw_false   = pp.CaselessKeyword('false').setParseAction(lambda t: 0)

        # Regex string representing the set of possible operators
        # Example : ">=|<=|!=|>|<|="
        OPERATOR_RX = "(?i)%s" % '|'.join([re.sub('\|', '\|', o) for o in Predicate.operators.keys()])

        # predicate
        field      = pp.Word(pp.alphanums + '_' + '.' + '-')
        operator   = pp.Regex(OPERATOR_RX).setName("operator")
        variable   = pp.Literal('$').suppress() + pp.Word(pp.alphanums + '_' + '.' + '-').setParseAction(lambda t: "$%s" % t[0])

        obj        = pp.Forward()
        value      = obj | pp.QuotedString('"') | pp.QuotedString("'") | kw_true | kw_false | integer | variable
            
        def handle_value_list(s, l, t):
            t = t.asList()
            new_t = [t]
            debug("[handle_value_list] s = %(s)s ** l = %(l)s ** t = %(t)s" % locals())
            debug("                    new_t = %(new_t)s" % locals())
            return new_t

        value_list = value \
                   | (pp.Literal("[").suppress() + pp.Literal("]").suppress()) \
                        .setParseAction(lambda s, l, t: [[]]) \
                   | pp.Literal("[").suppress() \
                   + pp.delimitedList(value) \
                        .setParseAction(handle_value_list) \
                   + pp.Literal("]") \
                        .suppress()

        table      = pp.Word(pp.alphanums + ':_-').setResultsName('object')
        field_list = pp.Literal("*") | pp.delimitedList(field).setParseAction(lambda tokens: set(tokens))

        assoc      = (field + pp.Literal(":").suppress() + value_list).setParseAction(lambda tokens: [tokens.asList()])
        obj        << pp.Literal("{").suppress() + pp.delimitedList(assoc).setParseAction(lambda t: dict(t.asList())) + pp.Literal("}").suppress()

        # PARAMETER (SET)
        # X = Y    -->    t=(X, Y)
        def handle_param(s, l, t):
            t = t.asList()
            assert len(t) == 2
            new_t = tuple(t)
            debug("[handle_param] s = %(s)s ** l = %(l)s ** t = %(t)s" % locals())
            debug("               new_t = %(new_t)s" % locals())
            debug("               (we expect a tuple)")
            return new_t

        param      = (field + pp.Literal("=").suppress() + value_list) \
            .setParseAction(handle_param)

        # PARAMETERS (SET)
        # PARAMETER[, PARAMETER[, ...]]    -->    dict()
        def handle_parameters(s, l, t):
            t = list(t.asList())
            new_t = dict(t) if t else dict()
            debug("[handle_parameters] s = %(s)s ** l = %(l)s ** t = %(t)s" % locals())
            debug("                    new_t = %(new_t)s" % locals())
            debug("                    (we expect a dict)")
            return new_t
        
        parameters     = pp.delimitedList(param) \
            .setParseAction(handle_parameters)

        predicate  = (field + operator + value_list).setParseAction(self.handlePredicate)

        # clause of predicates
        and_op     = pp.CaselessLiteral("and") | pp.Keyword("&&")
        or_op      = pp.CaselessLiteral("or")  | pp.Keyword("||")
        not_op     = pp.Keyword("!")

        predicate_precedence_list = [
            (not_op, 1, pp.opAssoc.RIGHT, lambda x: self.handleClause(*x)),
            (and_op, 2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x)),
            (or_op,  2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x))
        ]
        clause     = pp.operatorPrecedence(predicate, predicate_precedence_list) #.setParseAction(lambda clause: Filter.from_clause(clause))
        # END: clause of predicates

        # For the time being, we only support simple filters and not full clauses
        filter     = pp.delimitedList(predicate, delim='&&').setParseAction(lambda tokens: Filter(tokens.asList()))

        datetime   = pp.Regex(r'....-..-.. ..:..:..')

        timestamp  = pp.CaselessKeyword('now') | datetime

        select_elt = (kw_select.suppress() + field_list.setResultsName('fields'))
        where_elt  = (kw_where.suppress()  + filter.setResultsName('filters'))
        set_elt    = (kw_set.suppress()    + parameters.setResultsName('params'))
        at_elt     = (kw_at.suppress()     + timestamp.setResultsName('timestamp'))

        # SELECT *|field_list [AT timestamp] FROM table [WHERE clause]
        # UPDATE table SET parameters [WHERE clause] [SELECT *|field_list]
        # INSERT INTO table SET parameters  [SELECT *|field_list]
        # DELETE FROM table [WHERE clause]
        select     = (select_elt + pp.Optional(at_elt) + kw_from.suppress() + table + pp.Optional(where_elt)).setParseAction(lambda args: self.action(args, 'get'))
        update     = (kw_update + table + set_elt + pp.Optional(where_elt) + pp.Optional(select_elt)).setParseAction(lambda args: self.action(args, 'update'))
        insert     = (kw_insert + kw_into + table + set_elt + pp.Optional(select_elt)).setParseAction(lambda args: self.action(args, 'create'))
        delete     = (kw_delete + kw_from + table + pp.Optional(where_elt)).setParseAction(lambda args: self.action(args, 'delete'))

        self.bnf   = select | update | insert | delete

    def action(self, args, action):
        args['action'] = action

    def handlePredicate(self, args):
        return Predicate(*args)

    def handleClause(self, args):
        return Clause(*args)

    def parse(self, string):
        result = self.bnf.parseString(string, parseAll=True)
        #print result.dump()
        return dict(result.items())

if __name__ == "__main__":

    STR_QUERIES = [
        'UPDATE slice SET lease = [{resource: "urn:publicid:IDN+ple:certhple+node+iason.inf.uth.gr", start_time: 1392130800, duration: 2}] where slice_hrn == "ple.upmc.myslicedemo"',
        'SELECT ip_id, node_id AT now FROM node WHERE node_id included [8252]',
        'SELECT hops.ip, hops.ttl AT 2012-09-09 14:30:09 FROM traceroute WHERE agent_id == 11824 && destination_id == 1417 && test_field == "test"',
        'SELECT slice_hrn FROM slice',
        'SELECT slice_hrn, slice_description FROM slice WHERE slice_hrn == "ple.upmc.myslicedemo"',
        'UPDATE local:platform SET disabled = True, pouet = false WHERE platform == "ple"',
        'UPDATE local:platform SET disabled = False WHERE platform == "omf"',
    ]

    def eval(s):
        print "===== %s =====" % s
        print SQLParser().parse(s)
        query = Query(SQLParser().parse(s))
        print query

    if len(sys.argv) == 2:
        eval(sys.argv[1])
    else:
        for s in STR_QUERIES:
            eval(s)
