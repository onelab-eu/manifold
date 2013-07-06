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

        # Regex string representing the set of possible operators
        # Example : ">=|<=|!=|>|<|="
        OPERATOR_RX = '|'.join([re.sub('\|', '\|', o) for o in Predicate.operators.keys()])

        # predicate
        field = pp.Word(pp.alphanums + '_' + '.')
        operator = pp.Regex(OPERATOR_RX).setName("operator")
        value = pp.QuotedString('"') | integer ##| pp.Combine( pp.Word( "+-"+ pp.nums, pp.nums) + pp.Optional( point + pp.Optional( pp.Word( pp.nums ) ) ) + pp.Optional( e + pp.Word( "+-"+pp.nums, pp.nums ) ) )
        #value_list = pp.delimitedList(value).setParseAction(lambda tokens: tuple(tokens.asList()))
        value_list = value | pp.Literal("[").suppress() + pp.delimitedList(value).setParseAction(lambda tokens: tuple(tokens.asList())) + pp.Literal("]").suppress()
        

        predicate = (field + operator + value_list).setParseAction(self.handlePredicate)

        # clause of predicates
        and_op = pp.CaselessLiteral("and") | pp.Keyword("&&")
        or_op  = pp.CaselessLiteral("or")  | pp.Keyword("||")
        not_op = pp.Keyword("!")

        predicate_precedence_list = [
            (not_op, 1, pp.opAssoc.RIGHT, lambda x: self.handleClause(*x)),
            (and_op, 2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x)),
            (or_op,  2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x))
        ]
        clause = pp.operatorPrecedence(predicate, predicate_precedence_list).setParseAction(lambda pred: Filter(pred))

        kw_select = pp.CaselessKeyword('select')
        kw_update = pp.CaselessKeyword('update')
        kw_insert = pp.CaselessKeyword('insert')
        kw_delete = pp.CaselessKeyword('delete')
        
        kw_from   = pp.CaselessKeyword('from')
        kw_into   = pp.CaselessKeyword('into')
        kw_where  = pp.CaselessKeyword('where')
        kw_at     = pp.CaselessKeyword('at')
        kw_set    = pp.CaselessKeyword('set')

        field_list = pp.Literal("*") | pp.delimitedList(field).setParseAction(lambda tokens: set(tokens.asList()))

        table = pp.Word(pp.alphanums + ':_').setResultsName('object')


        params       = field_list
        timestamp    = pp.CaselessKeyword('now')

        select_elt   = (kw_select.suppress() + field_list.setResultsName('fields'))
        where_elt    = (kw_where.suppress()  + clause.setResultsName('filters'))
        set_elt      = (kw_set.suppress()    + params.setResultsName('params'))
        at_elt       = (kw_at.suppress()     + timestamp.setResultsName('timestamp'))

        # SELECT *|field_list [AT timestamp] FROM table [WHERE clause]
        # UPDATE table SET params [WHERE clause] [SELECT *|field_list]
        # INSERT INTO table SET params  [SELECT *|field_list]
        # DELETE FROM table [WHERE clause]

        select_query = (select_elt + pp.Optional(at_elt) + kw_from.suppress() + table + pp.Optional(where_elt)).setParseAction(lambda args: self.action(args, 'select'))
        update_query = (kw_update + table + set_elt + pp.Optional(where_elt) + pp.Optional(select_elt)).setParseAction(lambda args: self.action(args, 'update'))
        insert_query = (kw_insert + kw_into + table + set_elt + pp.Optional(select_elt)).setParseAction(lambda args: self.action(args, 'insert'))
        delete_query = (kw_delete + kw_from + table + pp.Optional(where_elt)).setParseAction(lambda args: self.action(args, 'delete'))
        # TODO execute_query  =

        self.bnf     = select_query | update_query | insert_query | delete_query#).setParseAction(self.handleQuery)

    def action(self, args, action):
        args['action'] = action

    def handlePredicate(self, args):
        return Predicate(*args)

    def handleClause(self, args):
        return Clause(*args)

    def handleQuery(self, args):
        print "HANDLEQUERY", args
        args['pouet'] = "pouet"

    def handleGet(self, args):
        if len(args) == 6:
            _, fields, _, object, _, filter = args
            query = Query.get(object).select(fields).filter_by(filter)
        else:
            _, fields, _, object = args
            query = Query.get(object).select(fields)
        return query

    def parse(self, string):
        return dict(self.bnf.parseString(string).items())#,parseAll=True)

if __name__ == "__main__":

    STR_QUERIES = [
        'SELECT slice_hrn FROM slice"',
        'SELECT slice_hrn FROM slice WHERE slice_hrn == "ple.upmc.myslicedemo"',
    ]

    for s in STR_QUERIES:
        print "===== %s =====" % s
        d = SQLParser().parse(s)
        print d
        query = Query(d)
        print query
