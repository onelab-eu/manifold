#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Clause describes complex conditions use for instance in the WHERE
# statement of a Query. A Clause is made of one or two operands
# depending on its operator. Supported operators are:
# - unary clauses: NOT, !
# - binary clauses: OR, ||, AND, &&
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr> 
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import pyparsing as pp
import re

from types                      import StringTypes
from operator                   import and_, or_, neg 

from manifold.util.predicate    import Predicate
from manifold.util.log          import Log
from manifold.util.type         import accepts, returns

# XXX When to use Keyword vs. Regex vs. CaselessLiteral
# XXX capitalization ?

# Instead of CaselessLiteral, try using CaselessKeyword. Keywords are better
# choice for grammar keywords, since they inherently avoid mistaking the leading
# 'in' of 'inside' as the keyword 'in' in your grammar.


class Clause(object):

    operators = {
        "&&"       : and_,
        "and"      : and_,
        "||"       : or_,
        "or"       : or_,
        "!"        : neg
    }

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], StringTypes):
            return ClauseStringParser().parse(args[0])
        return super(Clause, cls).__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        """
        Constructor
        """
        if len(args) == 2:
            # Unary
            operator = args[0].lower()
            operand  = args[1]
            assert operator == neg, "Invalid operator: %s" % operator
            self.operator = Clause.operators[operator]
            self.operands = [operand]
        elif len(args) == 3:
            # Binary
            self.operator = Clause.operators[args[1].lower()]
            self.operands = [args[0], args[2]]
            assert self.operator == or_ or self.operator == and_, "Invalid operator: %s" % self.operator
        else:
            raise ValueError, "Clause can only be unary or binary."

        for operand in self.operands:
            assert isinstance(operand, (Clause, Predicate)), "Invalid operand %s (%s)" % (operand, type(operand))

    def get_operator(self):
        """
        Returns:
            The operator applied to each operand of this clause
        """
        return self.operator

    @returns(StringTypes)
    def operator_to_string(self, operator):
        """
        Args:
            operator: An operator among and_, or_, neg
        Returns:
            The string corresponding to a Clause operator. 
        """
        for string, op in Clause.operators.items():
             if op == operator: return string
        return ''

    def get_left(self):
        """
        Returns:
            The single operand of this Clause.
            It may be either a Clause or a Predicate instance.
        """
        assert len(self.operands) == 1
        return self.operands[0]

    def get_left(self):
        """
        Returns:
            The left operand of this Clause.
            It may be either a Clause or a Predicate instance.
        """
        assert len(self.operands) == 2
        return self.operands[0]

    def get_right(self):
        """
        Returns:
            The right operand of this Clause.
            It may be either a Clause or a Predicate instance.
        """
        assert len(self.operands) == 2
        return self.operands[1]

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Clause.
        """
        if len(self.operands) == 1:
            return "(%r %r)" % (
                self.operator_to_string(self.get_operator()),
                self.get_operand()
            )
        else:
            return "(%r %s %r)" % (
                self.get_left(),
                self.operator_to_string(self.get_operator()),
                self.get_right()
            )

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this Clause.
        """
        if len(self.operands) == 1:
            return "Clause<%s>(%s)" % (
                self.operator_to_string(self.get_operator()),
                self.get_operand()
            )
        else:
            return "Clause<%s>(%s, %s)" % (
                self.operator_to_string(self.get_operator()),
                self.get_left(),
                self.get_right()
            )

class ClauseStringParser(object):

    def __init__(self):
        """
        BNF HERE
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

        self.bnf = clause

    def handlePredicate(self, args):
        return Predicate(*args)

    def handleClause(self, args):
        return Clause(*args)

    def parse(self, string):
        return self.bnf.parseString(string,parseAll=True)

if __name__ == "__main__":
    print ClauseStringParser().parse('country == "Europe" || (ts > "01-01-2007" && country == "France")')
    print Clause('country == "Europe" OR ts > "01-01-2007" AND country == "France"')
