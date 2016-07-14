#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import argparse
import logging as log
import coloredlogs

from collections import defaultdict
from mapper import json_to_edge


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {        
        'simple': { 'format': '%(levelname)s %(message)s'}
    },
}


def lexed_key(a,b):
    """Lexicographically sort nodes in an edge and return a tuple key"""
    if a < b:
        return (a,b)
    else:
        return (b,a)

def emit(value):
    """Workaround for Python's default rounding behavior"""
    str_deg = '%.3f' % value
    integer_deg, sep, decimal_deg = str_deg.partition('.')
    return '.'.join( [integer_deg, (decimal_deg+'0'*2)[:2]])


def main(args):

    lower_bound = 0    
    edges = {}    

    with open(args.input, 'r') as trans, open(args.output, 'w') as outfile:
        for i, raw in enumerate(trans): 
            ## Ingest, as raw "events" from file stream
            log.debug("raw:%i: %s" % (i, raw.rstrip()))     
            
            ## Map, raw to tuple/edge representation 
            a, b, timestamp = json_to_edge(raw)
            key = lexed_key(a,b)
            delta = timestamp - lower_bound

            ## Reduce edges
            if delta >= 0: # within window
                if delta >= args.window: # ahead of window
                    lower_bound = timestamp - args.window + 1
                    for edge in edges.keys():                        
                        if edges[edge] < lower_bound:
                            del edges[edge]                

                # observe current edge
                try:
                    # touch ttl                
                    if timestamp > edges[key]:
                        edges[key] = timestamp
                except KeyError:
                    # add edge
                    edges[key] = timestamp

            ## Reduce nodes
            nodes = defaultdict(int)
            for edge in edges.keys():
                a, b = edge
                nodes[a] += 1
                nodes[b] += 1

            # determine degree distribution
            dist = nodes.values()            
            dist.sort()
            log.debug("dist(%i): %s" % (len(dist), dist))

            # find median
            length = len(dist)
            if length % 2 == 0:
                idx = (length / 2) - 1
                median = sum(d for d in dist[idx:idx+2]) / 2.0
            else: 
                idx = (length - 1) / 2
                median = dist[idx]
            
            
            ## Collect, output                   
            log.info("output (median degree): %f" % median)
            print(emit(median), file=outfile)         
            
            log.debug("\n----------\n")  

           

# --------------------------------------------------------------------
class ArgparseFormatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass


if __name__ == '__main__':  

    parser = argparse.ArgumentParser(description=__doc__, 
                formatter_class=ArgparseFormatter)

    parser.add_argument('-i', '--input', required=True, nargs='?',
        const='./venmo_input/venmo-trans.txt',
        default='./venmo_input/venmo-trans.txt',
        help="file containing json transactions from venmo api")

    parser.add_argument('-o', '--output',
        default='./venmo_output/output.txt',
        help="output median vertex degrees, one per transaction")

    parser.add_argument('-w', '--window', type=int,
        default=60,
        help="sliding window (lagging) in seconds")

    # NOT IMPLEMENTED
    # parser.add_argument('-s', '--step', type=int,
    #     default=1,
    #     help="increment window in steps of size STEP")

    parser.add_argument('-d', '--debug', dest='log_lvl', nargs='?', 
        const='DEBUG',
        default='INFO',
        help="debug log verbosity [CRITICAL, ERROR, WARNING, INFO, DEBUG]")

    parser.add_argument('-q', '--quiet', dest='log_lvl', action='store_const', 
        const=100,      
        help="suppress all messages")

    if len(sys.argv[1:]) == 0:      
        parser.print_help()
        parser.exit()

    args = parser.parse_args()

    import logging.config
    logging.config.dictConfig(LOGGING)
    coloredlogs.install(level=args.log_lvl)

    main(args)