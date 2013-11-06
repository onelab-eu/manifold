#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.query     import Query
from manifold.core.filter    import Filter
from manifold.util.predicate import Predicate
from manifold.util.log       import Log
from manifold.util.clause    import Clause
import pyparsing as pp
import re
 
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
        field      = pp.Word(pp.alphanums + '_' + '.')
        operator   = pp.Regex(OPERATOR_RX).setName("operator")
        variable   = pp.Literal('$').suppress() + pp.Word(pp.alphanums + '_' + '.').setParseAction(lambda t: "$%s" % t[0])
        value      = pp.QuotedString('"') | pp.QuotedString("'") | kw_true | kw_false | integer | variable
        value_list = value | pp.Literal("[").suppress() + pp.delimitedList(value).setParseAction(lambda tokens: tuple(tokens.asList())) + pp.Literal("]").suppress()
        
        table      = pp.Word(pp.alphanums + ':_-').setResultsName('object')
        field_list = pp.Literal("*") | pp.delimitedList(field).setParseAction(lambda tokens: set(tokens))
        
        param      = (field + pp.Literal("=").suppress() + value_list).setParseAction(lambda tokens: tuple(tokens.asList()))
        params     = pp.delimitedList(param).setParseAction(lambda tokens: dict(tokens.asList()))

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
        set_elt    = (kw_set.suppress()    + params.setResultsName('params'))
        at_elt     = (kw_at.suppress()     + timestamp.setResultsName('timestamp'))

        # SELECT *|field_list [AT timestamp] FROM table [WHERE clause]
        # UPDATE table SET params [WHERE clause] [SELECT *|field_list]
        # INSERT INTO table SET params  [SELECT *|field_list]
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
        'SELECT ip_id, node_id AT now FROM node WHERE node_id included [8252]',
        'SELECT hops.ip, hops.ttl AT 2012-09-09 14:30:09 FROM traceroute WHERE agent_id == 11824 && destination_id == 1417 && test_field == "test"',
        'SELECT slice_hrn FROM slice',
        'SELECT slice_hrn, slice_description FROM slice WHERE slice_hrn == "ple.upmc.myslicedemo"',
        'UPDATE local:platform SET disabled = True, pouet = false WHERE platform == "ple"',
        'UPDATE local:platform SET disabled = False WHERE platform == "omf"',
    ]

    for s in STR_QUERIES:
        print "===== %s =====" % s
        print SQLParser().parse(s)
        query = Query(SQLParser().parse(s))
        print query
