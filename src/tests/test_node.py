
import unittest
from node import Node


class TestNode(unittest.TestCase):

    def test_node(self):
        self.assertEquals(Node(1), 1)

    def test_node_add(self):
        self.assertEquals(Node(1)+1, 2)
        self.assertEquals(Node(1)+Node(1), 2)

    def test_node_subtract(self):
        self.assertEquals(Node(1)-1, 0)
        self.assertEquals(Node(1)-Node(1), 0)

    def test_node_cmp(self):
        self.assertGreater(Node(1), Node(0))
        self.assertLess(Node(-1), Node(0))
        self.assertEquals(Node(1), Node(1))
        
