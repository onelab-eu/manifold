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
from manifold.policy.target.cache.entry import Entry
from manifold.util.log      import Log

class Policy(object):
    """
    The policy is defined by the set of local rules.
    """

    # Decisions
    ACCEPT      = 0
    REWRITE     = 1
    CACHE_HIT   = 2
    DENIED      = 3
    ERROR       = 4

    map_decision = {
        0: 'ACCEPT',
        1: 'REWRITE',
        2: 'CACHE_HIT',
        3: 'DENIED',
        4: 'ERROR',
    }

    def __init__(self, interface):
        self._interface = interface
        self.rules = []

    def load(self):
        Target.register_plugins()

        query_rules = Query.get('local:policy').select('policy_json')
        rules = self._interface.execute_local_query(query_rules)
        
        for rule in rules:
            self.rules.append(Rule.from_dict(json.loads(rule['policy_json'])))

    def filter(self, query, record, annotations, is_query = True):
        for rule in self.rules:
            if not rule.match(query, annotations):
                continue
            target = Target.get(rule.target)
            if not target:
                Log.warning("Unknown target %s" % rule.target)
                continue
            # TODO: ROUTERV2
            # Cache per user
            # Adding interface in order to access router.get_cache(annotations)
            decision, data = target(self._interface).process(query, record, annotations, is_query)
            if decision == TargetValue.ACCEPT:
                return (self.ACCEPT, None)
            elif decision == TargetValue.REWRITE:
                return (self.REWRITE, data)
            elif decision == TargetValue.CACHE_HIT:
                return (self.CACHE_HIT, data)
            elif decision == TargetValue.DENIED:
                return (self.DENIED, None)
            elif decision == TargetValue.ERROR:
                return (self.ERROR, data)
            elif decision == TargetValue.CONTINUE:
                continue

        # Let's create a cache entry
        if is_query:
            # We are dealing with queries
            cache = self._interface.get_cache(annotations)
            # XXX TEMP HACK
            try:
                cache.add_entry(query, Entry())
            except Exception,e: 
                Log.warning(e)

        # Default decision : ACCEPT
        return (self.ACCEPT, None)
