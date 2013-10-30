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

class Policy(object):
    """
    The policy is defined by the set of local rules.
    """

    def __init__(self, interface):
        self.interface = interface
        self.rules = []

    def load(self):
        Target.register_plugins()

        query_rules = Query.get('local:policy').select('policy_json')
        ret_rules = self.interface.forward(query_rules)
        if not ret_rules['code'] == 0:
            raise Exception, "Could not load policy rules"
        rules = ret_rules['value']
        
        for rule in rules:
            self.rules.append(Rule.from_dict(json.loads(rule['policy_json'])))
        
    def filter(self, query, annotation):
        count = 0
        loop = True

        while loop:
            loop = False
            for rule in self.rules:
                if not rule.match(query, annotation):
                    continue

                targetvalue = Target.get(rule.target)().process(query, annotation)

                if targetvalue == TargetValue.CONTINUE:
                    continue
                elif targetvalue == TargetValue.DROP:
                    return False
                elif targetvalue == TargetValue.STOLEN:
                    return False
                elif targetvalue == TargetValue.ACCEPT:
                    return True
                elif targetvalue == TargetValue.REPEAT:
                    count += 1
                    if count > 5:
                        raise Exception, "Possible infinite loop detected in rules: made 5 iterations so far..."
                    loop = True

        # Default decision : ACCEPT
        return True

            
