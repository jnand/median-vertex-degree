"""EdgeTimeCache is a LRU-based rolling-window edge-cache.
-----

This structure provides a Least-Recently-Used cache based on a fixed-length time
window. The cache size is pre-allocated to a fixed length on initialization.
Each array index then corresponds to a cache bucket, in this case a time-stamp
delta. When the cache is updated: new records ahead of the window trigger an
eviction of all trailing buckets greater than the updated lower-bound delta, current
records within the window touch the hash-key within an existing bucket, and old 
records behind the window do nothing (emitting an empty "diff list").

"""

import logging as log
from itertools import chain
from collections import defaultdict

class Cache(object):
    """Mange the addition and eviction of graph edges from the EdgeTimeCache.

    Args:
        size (int): number of buckets to allocate in cache window

    """

    def __init__(self, size=60, **kwargs):
        self._lower_bound = 0 #time-stamp
        self._size = size       
        self._rolling_window = list([set() for i in xrange(size)])
        self._edges = {} # key: int bucket_number


    @property
    def size(self):
        return self._size

    @property 
    def cache(self):
        return self._rolling_window

    @property
    def edges(self):
        return self._edges

    @property
    def lower_bound(self):
        return self._lower_bound

    def update_lower_bound(self, timestamp):
        """Advance window forward."""
        self._lower_bound = timestamp - self.size + 1 # +1 shifts to the next "exclusive" frame
        log.debug("lower_bound-> %i-%i: %i" % (timestamp, self.size, self._lower_bound))


    @staticmethod
    def lexed_key(a,b):
        """Lexicographically sort nodes in an edge and return a tuple key."""
        if a < b:
            return (a,b)
        else:
            return (b,a)


    @staticmethod
    def dict_sum(a,b):
        """Add dict b to dict a, summing values."""
        for k, v in b.iteritems():
            a[k] += v
        return a


    def delta(self, timestamp): 
        """Find location winint the time window."""       
        delta = timestamp - self.lower_bound
        log.debug("delta-> %i-%i: %i" % (timestamp, self.lower_bound, delta))
        return  delta


    def update(self, edge):
        """Update the cache with an edge observation.

        Args:
            edge tuple(str, str, int): pre-parsed edge, where the 3rd parameter 
                is the time-stamp in unix epoch.

        """
        (a, b, timestamp) = edge
        key   = self.lexed_key(a, b)
        delta = self.delta(timestamp)
        
        diff = defaultdict(int)
        # Within window
        if delta >= 0:
            # New, ahead of window, trigger cache eviction
            if delta >= self.size:                 
                self.update_lower_bound(timestamp)
                evicted = self.evict_expired(delta)                
                delta   = self.delta(timestamp)                
                diff.update(evicted)
            
            # Current, add edge to cache
            new  = self.observe_edge(delta, key, timestamp)
            diff = self.dict_sum(diff, new)

        # Old, behind window, delta < 0, Do nothing

        return diff


    def truncate(self, index):
        """Roll trailing buckets, less than index, off the end of the cache."""
        res = self.cache[0:index]        
        del(self.cache[0:index])        
        self.cache.extend([set() for i in xrange(len(res))])  
        return res


    def evict_expired(self, delta):
        """Handles the removal of stale edges and bookkeeping operations of related structures."""
        
        index = delta - self.size + 1        
        truncated = self.truncate(index)        
        flattened = list(chain.from_iterable(truncated))
        
        evicted = {}
        for edge in flattened:
            # only evict edges from cache that are behind the new lower_bound
            if self.edges.has_key(edge) and (self.edges[edge] < self.lower_bound):
                evicted[edge] = -1
                del self.edges[edge]

        log.debug("evicted: %s" % evicted)
        log.debug("truncated: %s" % self.cache)
        return evicted

        
    def observe_edge(self, bucket, key, timestamp):
        """Apply operations to incoming edge."""
        obs = {}
        if self.edges.has_key(key):
            if timestamp > self.edges[key]:
                self.edges[key] = timestamp            
        else:            
            self.edges[key] = timestamp
            obs = {key: 1}

        self.cache[bucket].add(key)
        return obs

