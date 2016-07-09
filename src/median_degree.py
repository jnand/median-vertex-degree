#!/usr/bin/env python
"""Calculate the median vertex degree of a Venmo payment graph."""

from __future__ import print_function

import os
import sys
import argparse
import logging as log
import coloredlogs

# Debugging
import pprint as pp
# import ipdb; ipdb.set_trace()


LOGGING = {
	'version': 1,
	'disable_existing_loggers': False,
	'formatters': {        
        'simple': { 'format': '%(levelname)s %(message)s'}
    },
}


# --------------------------------------------------------------------
def main(args):
	pass

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

	parser.add_argument('-s', '--step', type=int,
		default=1,
		help="increment window in steps of size STEP")

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