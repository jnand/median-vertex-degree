"""Map helpers for processing raw input into structured data."""


import time
import logging as log
import ujson as json

from datetime import datetime


TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
EPOCH = datetime(1970,1,1)


# Convert time to epoch
def parse_time_string(string):
    """Convert time format into unix epoch."""
    try:        
        time = datetime.strptime(string, TIME_FORMAT)
        timestamp = (time - EPOCH).total_seconds()
    except TypeError:
        timestamp = 0
    return int(timestamp)


def json_to_edge(string):
    """Transform raw json to edge tuple."""
    data = json.loads(string)
    timestamp = parse_time_string(data.get('created_time'))
    actor = data.get('actor')
    target = data.get('target')
    log.debug("parsed(edge): [%s] %s -> %s" % (timestamp,actor,target))
    return (actor, target, timestamp)

