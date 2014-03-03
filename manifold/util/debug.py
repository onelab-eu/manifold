# -*- coding: utf-8 -*-

import traceback

def print_call_stack():
    for line in traceback.format_stack():
        print line.strip()
