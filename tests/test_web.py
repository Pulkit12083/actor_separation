import unittest
from actor_separation.web import get_json_data
from actor_separation.web import check_valid_url

class TestWeb(unittest.TestCase):
    def test_get_json_data(self):
        self.assertEqual(get_json_data("NonActor"), None)  # add assertion here

    def test_check_valid_url(self):
        self.assertEqual(check_valid_url("amitabh-bachchan"), True)
        self.assertEqual(check_valid_url("nonactorurl"), False)

if __name__ == '__main__':
    unittest.main()
