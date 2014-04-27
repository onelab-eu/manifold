import unittest
from manifold.core.result_value import ResultValue

class ManifoldTestCase(unittest.TestCase):

    def assert_rv_success(self, result_value):
        assert isinstance(result_value, ResultValue)
        assert result_value.is_success()
        return result_value.get_value()
