# -*- coding: utf-8 -*-
#
# Policy support
#
# This file is part of the Manifold project
#
# Copyright (C)2009-2013, UPMC Paris Universitas
# Authors:
#   Jordan Augé <jordan.auge@lip6.fr>

import json

from manifold.core.query    import Query
from manifold.policy.target import Target, TargetValue
from manifold.policy.rule   import Rule
from manifold.util.log      import Log

class Policy(object):
    """
    The policy is defined by the set of local rules.
    """

    # Decisions
    ACCEPT  = 0
    REWRITE = 1
    RECORDS = 2
    DENIED  = 3
    ERROR   = 4

    def __init__(self, interface):
        self.interface = interface
        self.rules = []

    def load(self):
        Target.register_plugins()

        query_rules = Query.get('local:policy').select('policy_json')
        rules = self.interface.execute_local_query(query_rules)
        
        for rule in rules:
            self.rules.append(Rule.from_dict(json.loads(rule['policy_json'])))

    def filter(self, query, record, annotations):
        for rule in self.rules:
            if not rule.match(query, annotations):
                continue
            target = Target.get(rule.target)
            if not target:
                Log.warning("Unknown target %s" % rule.target)
                continue
            decision, data = target().process(query, record, annotations)
            if decision == TargetValue.ACCEPT:
                return (self.ACCEPT, None)
            elif decision == TargetValue.REWRITE:
                return (self.REWRITE, data)
            elif decision == TargetValue.RECORDS:
                return (self.RECORDS, data)
            elif decision == TargetValue.DENIED:
                return (self.DENIED, None)
            elif decision == TargetValue.ERROR:
                return (self.ERROR, data)
            elif decision == TargetValue.CONTINUE:
                continue

        # Default decision : ACCEPT
        return (self.ACCEPT, None)
            
