#!/usr/bin/env python

import json

from manifold.core.router import Router
from manifold.core.query import Query

from manifold.policy.rule        import Rule
from manifold.policy.target.drop import DropTarget

# A rule to prevent read access to a user's password

rule = Rule()

rule.object = 'local:user'
rule.fields = set(['password'])
rule.access = 'R'
rule.target = 'DROP'

rule_dict = rule.to_dict()
rule_params = {
    'policy_json': json.dumps(rule_dict)
}

query = Query(action='create', object='local:policy', params=rule_params)

# Instantiate a TopHat router
with Router() as router:
    router.forward(query)



