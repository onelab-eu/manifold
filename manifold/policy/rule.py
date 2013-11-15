#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Rule for the policy engine
#
# This file is part of the Manifold project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé <jordan.auge@lip6.fr>

# NOTE: this is a proof-of-concept of providing netfilter like rules for
# queries.

class Rule(object):
    """
    The policy is defined by the set of local rules.
    """

    @staticmethod
    def from_dict(rule_dict):
        # XXX missing input/output interfaces, match module, etc.
        rule = Rule()
        rule.object  = rule_dict['object']     if 'object'  in rule_dict else None
        rule.fields = set(rule_dict['fields']) if 'fields' in rule_dict else None 
        rule.access = rule_dict['access']      if 'access' in rule_dict else None 
        rule.target = rule_dict['target']      if 'target' in rule_dict else None
        return rule

    def __init__(self):
        self.object  = None
        self.fields = None
        self.access = None

        self.target = None

    def to_dict(self):
        return {
            'object' : self.object,
            'fields': list(self.fields),
            'access': self.access,
            'target': self.target
        }
    
    def match(self, query, annotation):
        """
        Match a query, annotation pair against the current rule
        """
        # XXX Note that we are not inspecting 'action' 

        if self.object != '*' and not query.object == self.object:
            return False


        query_fields_R   = set()
        query_fields_R  |= query.get_select()
        query_fields_R  |= query.get_where().get_field_names()

        query_fields_W   = set()
        query_fields_W  |= set(query.get_params().keys())

        query_fields_RW  = set()
        query_fields_RW |= query_fields_R
        query_fields_RW |= query_fields_W

        if self.access == 'R':
            return ('*' in self.fields and query_fields_R) or query_fields_R.intersection(self.fields)
        elif self.access == 'W':
            return ('*' in self.fields and query_fields_W) or query_fields_W.intersection(self.fields)
        elif self.access == 'RW':
            return ('*' in self.fields and query_fields_RW) or query_fields_RW.intersection(self.fields)
