
import unittest
import mapper


class TestMapper(unittest.TestCase):

    def test_parse_time(self):
        result = mapper.parse_time_string("2016-03-29T02:05:17Z")
        self.assertEqual(result, 1459217117)
        with self.assertRaises(ValueError):
            mapper.parse_time_string("2016-03-09T02:13:17.0123Z")


    def test_json_to_edge(self):
        json = '{"created_time": "2016-03-29T02:15:39Z", "target": "target_a", "actor": "actor_b"}'
        expect = (u'actor_b',u'target_a',1459217739)
        result = mapper.json_to_edge(json)
        self.assertEqual(result, expect)