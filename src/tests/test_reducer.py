
import unittest
from reducer import Reducer


class TestReducer(unittest.TestCase):

    def setUp(self):
        self.reduce = Reducer()     


    def test_insert_node(self):
        nodes = self.reduce.nodes

        self.reduce.upsert_node('a', 1)
        self.assertEquals(nodes['a'], 1)

        self.reduce.upsert_node('b', 1)
        self.assertEquals(len(nodes.keys()), 2)


    def test_update_node(self):
        self.reduce.upsert_node('a', 1)
        self.reduce.upsert_node('a', 1)     
        self.assertEquals(self.reduce.nodes['a'], 2)

        self.reduce.upsert_node('a', -1)
        self.assertEquals(self.reduce.nodes['a'], 1)

        self.reduce.upsert_node('a', -1)
        with self.assertRaises(KeyError):
            self.reduce.nodes['a']


    def test_invalid_insert(self):
        with self.assertRaises(ValueError):
            self.reduce.upsert_node('a', -1)


    def test_update(self):
        initial_data = { ('a','b'): 1, ('c','d'): 1, ('e','f'): 1,
                         ('g','h'): 1, ('i','j'): 1, ('k','l'): 1 }
        self.assertEquals(self.reduce.update(initial_data), 1)

        updated_data = { ('c','d'): 1, ('e','f'): -1, ('k','l'): 1 }
        self.assertEquals(self.reduce.update(updated_data), 1)
        self.assertEqual(len(self.reduce.nodes), 10)
        with self.assertRaises(KeyError):
            self.reduce.nodes['e']
        with self.assertRaises(KeyError):
            self.reduce.nodes['f']

        updated_data = { ('a','d'): 1, ('e','d'): 1, ('g','i'): 1, ('k','l'): -1 }
        self.assertEquals(self.reduce.update(updated_data), 1)
        self.assertEqual(len(self.reduce.nodes), 11)        
        with self.assertRaises(KeyError):
            self.reduce.nodes['f']
        
