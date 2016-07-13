import os
import unittest
import logging

from median_degree import Cache, Reducer, json_to_edge, emit


# Disable logging
logging.disable(logging.CRITICAL)


class TestExample(unittest.TestCase):

    def setUp(self):    
        base_dir =  os.path.dirname(__file__)
        data_in  = os.path.join(base_dir, 'data/example/input.txt')
        data_out = os.path.join(base_dir, 'data/example/output.txt') 
        self.stream = open(data_in, 'r')
        self.expect = open(data_out, 'r')


    def test_example(self):     
        cache   = Cache()
        reducer = Reducer()

        for raw in self.stream:         
            edge   = json_to_edge(raw) # Map, raw to tuple/edge representation          
            diff   = cache.update(edge) # Partition, updates by cache bucket        
            result = reducer.update(diff) # Reduce, updates into one value per input event              
            degree = emit(result) # Collect, output

            expect = self.expect.readline().strip(os.linesep)
            self.assertEquals(degree, expect)
        

    def tearDown(self):
        self.stream.close()
        self.expect.close()


if __name__ == '__main__':
    unittest.main()