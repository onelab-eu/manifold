#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from tests                      import ManifoldTestCase

from manifold.core.record       import Record
from manifold.core.result_value import ResultValue
from manifold.bin.shell         import Shell

class GatewaysSFATests(ManifoldTestCase):

    def setUp(self):
        self.shell = Shell(interactive = False)
        self.shell.select_auth_method('local')

    def assertSuccess(self, ret):
        if not isinstance(ret, ResultValue):
            raise AssertionError, "Expected ResultValue"
        print ret
        try:
            records = ret.get_all()
        except Exception, e:
            raise AssertionError, str(e)
        if not isinstance(records, list):
            raise AssertionError, "Expected list of records, got %s" % (records.__class__, )

    def assertSuccessNotEmpty(self, ret):
        self.assertSuccess(ret)
        records = ret.get_all()
        if not records:
            raise AssertionError, "Empty list of records"
        record = records[0]
        if not isinstance(record, Record):
            raise AssertionError, "Expected record"
    
    def assertRecordKeyExists(self, ret, key):
        self.assertSuccessNotEmpty(ret)
        records = ret.get_all()
        record = records[0]
        if not key in ret:
            raise AssertionError, "Missing key %s in record" % (key, )

    def test_resources(self):
        cmd = "SELECT hrn FROM resource"
        ret = self.shell.evaluate(cmd)
        self.assertRecordKeyExists(ret, 'hrn')
        
    def tearDown(self):
        self.shell.terminate()

def main():
    unittest.main()

if __name__ == '__main__':
    main()
