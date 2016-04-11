# Spring 2016
# Ben Jones
#
# constants.py: list of constants for the script

MASTER_TIMEOUT_TO_END = 30
MASTER_WAIT_TIME = 5
SKY_TYPES = {'noop': 0, 'new_sky': 1, 'remove_sky': 2, 'no_data': 3}
SKY_TYPES_REV = {0: 'noop', 1: 'new_sky', 2: 'remove_sky', 3: 'no_data'}
WORKER_REQUERIES = 30
WORKER_MASTER_WAIT = 30
SERVER_TIMEOUT = 5
SERVER_REQUERIES = 5
