import unittest
from manifold.core.query import Query

#---------------------------------------------------------------------------
# Constructor
#---------------------------------------------------------------------------

class TestConstructor(unittest.TestCase):
    """
    Tests for manifold.core.query.Query
    """

    def test_empty(self):
        self.assertIsInstance(Query(), Query)

if __name__ == '__main__':
    unittest.main()
