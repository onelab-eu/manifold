#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Provide with statement to interrupt function taking more than
# N seconds.
# 
# Authors:
#   See http://stackoverflow.com/questions/366682/how-to-limit-execution-time-of-a-function-call-in-python

import signal
from contextlib import contextmanager

class TimeoutException(Exception):
    pass

@contextmanager
def time_limit(seconds):
    """
    with time_limit(...) statement.
    Example:
        try:
            with time_limit(10):
                long_function_call()
        except TimeoutException, msg:
            print "Timed out!"

    Args:
        timeout: An positive integer corresponding to
            the timeout (in seconds).
    """
    def signal_handler(signum, frame):
        raise TimeoutException, "Timed out!"

    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
