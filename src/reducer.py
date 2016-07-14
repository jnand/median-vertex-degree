"""Reducer step applies and memoizes mapped changes to the degree distribution."""

import logging as log
from blist import sortedlist


class Reducer(object):

    def __init__(self):
        self.nodes = {}
        self.degree_dist = sortedlist()


    def upsert_node(self, node, val):
        """Update, Insert, or Delete a node as needed."""
        try:                        
            self.degree_dist.remove(self.nodes[node])
            self.nodes[node] += val         
            
            if self.nodes[node] == 0:                
                del self.nodes[node]
            else:
                # replace node degree, with new value
                self.degree_dist.add(self.nodes[node])              
        except KeyError:            
            if val > 0: # only create entries for new nodes
                self.nodes[node] = val
                self.degree_dist.add(val)
            else:       
                raise ValueError('can not create new nodes with zero or negative values')
                

    def median(self):
        """Retrieve median degree from distribution."""
        log.debug("deg_dist(len:%i): %s" % (len(self.degree_dist), self.degree_dist))
        length = len(self.degree_dist)
        if length % 2 == 0: # even
            index = (length / 2) - 1
            return sum(d for d in self.degree_dist[index:index+2]) / 2.0
        else: # odd         
            index = (length - 1) / 2
            return self.degree_dist[index]          


    def update(self, changes):
        """Update the node-cache."""
        for (a, b), change in changes.iteritems():          
            if change != 0:
                self.upsert_node(a, change)
                self.upsert_node(b, change)     
        return self.median()

            