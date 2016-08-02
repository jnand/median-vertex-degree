#!/usr/bin/env python
"""Calculate the median vertex degree of a real-time streaming Venmo payment graph."""

from __future__ import print_function

import os
import sys
import argparse
import logging as log
import coloredlogs

from mapper import json_to_edge
from edge_time_cache import Cache
from reducer import Reducer

# Debugging
# import pprint as pp
# import ipdb; ipdb.set_trace()


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {        
        'simple': { 'format': '%(levelname)s %(message)s'}
    },
}


# -------------
def emit(value):
    """Workaround for Python's default rounding behavior"""
    str_deg = '%.3f' % value
    integer_deg, sep, decimal_deg = str_deg.partition('.')
    return '.'.join( [integer_deg, (decimal_deg+'0'*2)[:2]])


def main(args):
    lru_edge_cache  = Cache(size=args.window)
    reduce_node_deg = Reducer()

    if args.control:
        control = open(args.control, 'r')

    with open(args.input, 'r') as trans, open(args.output, 'w') as outfile:
        for i, raw in enumerate(trans): 

            # Input control supplied
            expect = control.readline().rstrip() if args.control else 'NA'

            # Ingest, as raw "events" from file stream
            log.debug("raw:%i: %s" % (i, raw.rstrip()))     
            
            # Map, raw to tuple/edge representation 
            edge = json_to_edge(raw)

            # 1st Reduce, updates by cache bucket
            diff = lru_edge_cache.update(edge)          

            # 2nd Reduce, updates into one value per input event
            result = reduce_node_deg.update(diff)
            
            # Collect, output  
            emit_result = emit(result)
            log.info("output (median degree): r= %s  e= %s" % (emit_result,expect))            
            print(emit(result), file=outfile)           
    
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

    parser.add_argument('-c', '--control', nargs='?',
        const='./venmo_output/control.txt',
        help="input control")

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