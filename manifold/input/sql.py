#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.query     import Query
from manifold.core.filter    import Filter
from manifold.util.predicate import Predicate
from manifold.util.log       import Log
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
        kw_false   = pp.CaselessKeyword('false') .setParseAction(lambda t: 0)

        # Regex string representing the set of possible operators
        # Example : ">=|<=|!=|>|<|="
        OPERATOR_RX = '|'.join([re.sub('\|', '\|', o) for o in Predicate.operators.keys()])

        # predicate
        field      = pp.Word(pp.alphanums + '_' + '.')
        operator   = pp.Regex(OPERATOR_RX).setName("operator")
        value      = pp.QuotedString('"') | kw_true | kw_false | integer 
        value_list = value | pp.Literal("[").suppress() + pp.delimitedList(value).setParseAction(lambda tokens: tuple(tokens.asList())) + pp.Literal("]").suppress()
        
        table      = pp.Word(pp.alphanums + ':_').setResultsName('object')
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
        clause     = pp.operatorPrecedence(predicate, predicate_precedence_list).setParseAction(lambda pred: Filter(pred))

        timestamp  = pp.CaselessKeyword('now')

        select_elt = (kw_select.suppress() + field_list.setResultsName('fields'))
        where_elt  = (kw_where.suppress()  + clause.setResultsName('filters'))
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
