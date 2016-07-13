
import unittest
from edge_time_cache import Cache


class TestCache(unittest.TestCase):

    def setUp(self):        
        self.cache = Cache()


    def test_lexed_key(self):
        result = self.cache.lexed_key('second','first')
        expect = ('first', 'second')
        self.assertEquals(result, expect)


    def test_cache(self):
        for bucket in self.cache.cache:
            self.assertEquals(type(bucket), type(set()))        


    def test_delta(self):
        self.assertEquals( self.cache.delta(1000000000), 1000000000)
        self.cache.update_lower_bound(1000000060)
        # tests for "exclusive" window, where a window currently at 60, starts 
        # at 1, NOT 0 
        self.assertEquals( self.cache.delta(1000000001), 0) 


    def test_truncate(self):    
        cache = self.cache.cache
        size  = self.cache.size
        self.assertEquals( len(cache), size)

        def hits(query):
            hits = 0
            for bucket in cache:
                if query in bucket:
                    hits += 1   
            return hits

        # mock cache cache
        n = 25
        mock_edge = ('a','b')
        cache[0:n] = [set([mock_edge])] * n
        self.assertEquals( len(cache), size) 
        self.assertEquals(hits(mock_edge), n)

        self.cache.truncate(n)
        self.assertEquals( len(cache), size)
        self.assertEquals(hits(mock_edge), 0)
        

    # def test_update(self):
    #   cache = self.cache
    #   edges = {
    #       ('c','d',20): {('c','d'): 1}, 
    #       ('c','e',40): {('c','d'): 1, ('c','e'): 1}, 
    #       ('c','f',60): {('c','d'): 1, ('c','e'): 1, ('c','f'): 1,}, 
    #       ('c','g',80): {('c','d'): -1, ('c','e'): 1, ('c','f'): 1, ('c','g'): 1, }, 
    #       ('d','e',100): 0,
    #       ('d','f',120): 0, 
    #       ('d','g',110): 0, 
    #       ('e','f',185): 0, 
    #       ('e','g',200): 0, 
    #       ('f','g',210): 0
    #   }

    #   import ipdb; ipdb.set_trace()
    #   for edge, expects in edges.iteritems():
    #       diff = cache.update(edge)
        



if __name__ == '__main__':
    unittest.main()     