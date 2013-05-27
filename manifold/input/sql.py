#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.query     import Query
from manifold.util.predicate import Predicate
import pyparsing as pp
import re

class SQLParser(object):

    def __init__(self):
        """
        Our simple BNF:
        SELECT [fields[*] FROM table WHERE clause
        """

        #integer = pp.Word(nums)
        #floatNumber = pp.Regex(r'\d+(\.\d*)?([eE]\d+)?')
        point = pp.Literal( "." )
        e     = pp.CaselessLiteral( "E" )

        # Regex string representing the set of possible operators
        # Example : ">=|<=|!=|>|<|="
        OPERATOR_RX = '|'.join([re.sub('\|', '\|', o) for o in Predicate.operators.keys()])

        # predicate
        field = pp.Word(pp.alphanums + '_')
        operator = pp.Regex(OPERATOR_RX).setName("operator")
        value = pp.QuotedString('"') #| pp.Combine( pp.Word( "+-"+ pp.nums, pp.nums) + pp.Optional( point + pp.Optional( pp.Word( pp.nums ) ) ) + pp.Optional( e + pp.Word( "+-"+pp.nums, pp.nums ) ) )

        predicate = (field + operator + value).setParseAction(self.handlePredicate)

        # clause of predicates
        and_op = pp.CaselessLiteral("and") | pp.Keyword("&&")
        or_op  = pp.CaselessLiteral("or")  | pp.Keyword("||")
        not_op = pp.Keyword("!")

        predicate_precedence_list = [
            (not_op, 1, pp.opAssoc.RIGHT, lambda x: self.handleClause(*x)),
            (and_op, 2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x)),
            (or_op,  2, pp.opAssoc.LEFT,  lambda x: self.handleClause(*x))
        ]
        clause = pp.operatorPrecedence(predicate, predicate_precedence_list)

        kw_select = pp.CaselessKeyword('select')
        kw_from   = pp.CaselessKeyword('from')
        kw_where  = pp.CaselessKeyword('where')

        fields = field + pp.ZeroOrMore(',' + pp.Optional(pp.White()) + field)
        table = pp.Word(pp.alphanums + '_')
        query = (kw_select + fields + kw_from + table + pp.Optional(kw_where + clause)).setParseAction(self.handleGet)

        self.bnf = query

    def handlePredicate(self, args):
        return Predicate(*args)

    def handleClause(self, args):
        return Clause(*args)

    def handleGet(self, args):
        _, fields, _, object, _, filter = args
        return Query.get(object).select(fields).filter_by(filter)

    def parse(self, string):
        return self.bnf.parseString(string,parseAll=True)

if __name__ == "__main__":
    query, = SQLParser().parse('SELECT slice_hrn FROM slice WHERE slice_hrn == "ple.upmc.myslicedemo"')
    print query.object
