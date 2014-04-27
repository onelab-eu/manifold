#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest, inspect
from manifold.core.record       import Record

class TestGetValue(unittest.TestCase):
    """
    Test for manifold.core.record.Record.get_value
    """

    #---------------------------------------------------------------------------
    # Helpers
    #---------------------------------------------------------------------------

    def _compare(self, input_dict, fields, expected_value):
        record = Record(input_dict)

        if inspect.isclass(expected_value) and issubclass(expected_value, Exception):
            with self.assertRaises(expected_value):
                record.get_value(fields)
        else:
            self.assertEquals(record.get_value(fields), expected_value)
        

    #---------------------------------------------------------------------------
    # Tests
    #---------------------------------------------------------------------------

    def test_nodot(self):
        self._compare({'a': 1}, 'a', 1)

    def test_nodot_notfound(self):
        self._compare({'a': 1}, 'b', KeyError)

    def test_dot_notfound(self):
        pass

if __name__ == '__main__':
    unittest.main()
