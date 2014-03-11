#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from tests                      import ManifoldTestCase
from manifold.core.query        import Query
from manifold.operator.subquery import SubQuery

#TEST_SUBQUERY_1 = Query.get('traceroute').select('source', 'destination', 'hops.ip', 'hops.country_name')
#
#def test_split_query():
#    sq = SubQuery(None, None)
#    parent_query, children_query_list = sq.split_query(TEST_SUBQUERY_1)
#    self.assert()
