import unittest
from actor_separation.graph import find_degrees_of_separation

class TestGraph(unittest.TestCase):
    def test_find_degrees_of_separation_0(self):
        self.assertEqual(find_degrees_of_separation("amitabh-bachchan",
                                                    "amitabh-bachchan"),
                         0)

    def test_find_degrees_of_separation_1(self):
        self.assertEqual(find_degrees_of_separation("amitabh-bachchan",
                                                    "leonardo-dicaprio"),
                         1)


if __name__ == '__main__':
    unittest.main()
