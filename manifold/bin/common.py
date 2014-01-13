#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions useful to write manifold scripts. 
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys

from traceback           import format_exc
from types               import StringTypes
from manifold.bin.shell  import Shell
from manifold.util.log   import Log 
from manifold.util.type  import accepts, returns

CODE_SUCCESSFUL         =  0

# Those values should be < 0 to avoid collisions with codes
# defined in manifold.core.code
CODE_ERROR_PARSING      = -1
CODE_ERROR_BAD_NUM_ARGS = -2

@returns(bool)
def check_num_arguments(error_message, argc_min, argc_max = None):
    """
    Check whether the number of argument passed the command line is correct.
    If not, leave the program with error code CODE_ERROR_BAD_NUM_ARGS.
    Args:
        error_message: A format String.
            Example:
                "%(program_name)s has failed: %(default_message)s"
            Supported format are:
                %(default_message)s : the default error message
                %(program_name)s    : the name of the running program
                %(argc_min)s        : see argc_min
                %(argc_max)s        : see argc_max
        argc_min: The minimal number of argument that must be passed to
            the command-line, including the program name (so this
            value is always >= 1).
        argc_max: The maximal number of argument that must be passed to
            the command-line, including the program name. This value
            must always be >= argc_min. You may pass None if unbound
    """
    assert argc_min >= 1,\
        "Invalid argc_min = %s (should be >= 1)" % argc_min
    assert isinstance(error_message, StringTypes),\
        "Invalid error_message = %s (%s)" % (error_message, type(error_message))
    assert isinstance(argc_min, int),\
        "Invalid argc_min = %s (%s)" % (argc_min, type(argc_min))
    assert not argc_max or isinstance(argc_max, int),\
        "Invalid argc_max = %s (%s)" % (argc_max, type(argc_max))
    assert not argc_max or argc_max >= argc_min,\
        "Invalid argc_max = %s (should be >= argc_min = %s)" % (argc_max, argc_min)

    argc = len(sys.argv)
    invalid_argc = True
    if argc < argc_min:
        default_message = "Not enough arguments (got %(argc)s argument(s), expected at least %(argc_min)s argument(s))" % locals()
    elif argc_max and argc > argc_max:
        default_message = "Too many arguments (got %(argc)s argument(s), expected at most %(argc_max)s argument(s))" % locals()
    else:
        invalid_argc = False

    if invalid_argc:
        program_name = sys.argv[0]
        Log.error(error_message % locals())
        sys.exit(CODE_ERROR_BAD_NUM_ARGS)

@returns(int)
@accepts(StringTypes, dict)
def run_command(command, args):
    """
    Pass a command to a non-interactive Manifold Shell.
    Args:
        command: A format String.
            Example:
                'SELECT * FROM foo WHERE foo_id == "%(foo_id)s"'
        args: A dictionnary used to fill the format String.
            Examples:
                {"foo_id" : 1}
                locals()
    Returns:
        CODE_ERROR_PARSING: if the command is not well-formed 
        0                 : iif successful
        >0                : if the command has failed
            See also manifold.core.code
    """
    Shell.init_options()
    shell = Shell(interactive = False)

    try:
        result_value = shell.evaluate(command % args)
        is_success = result_value.is_success()
        ret = CODE_SUCCESSFUL if is_success else result_value.get_code() 
    except Exception, e:
        Log.error(format_exc())
        ret = CODE_ERROR_PARSING

    shell.terminate()
    return ret 

