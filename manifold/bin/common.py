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
CODE_ERROR_INVALID_ARG  = -2
CODE_ERROR_BAD_NUM_ARGS = -3

MSG_SUCCESS = "Success: %(command)s"

MSG_INVALID_ARG = """
Invalid value passed for %(option_name)s (got '%(option_value)s')
"""

MSG_INVALID_ARG_ENUM = """
Invalid value passed for %(option_name)s (got '%(option_value)s')
Supported values are: %(valid_values)s
"""

MSG_NOT_ENOUGH_ARGS = """
Not enough arguments (got %(argc)s argument(s)).
Expected at least %(argc_min)s argument(s))
"""

MSG_TOO_MANY_ARGS = """
Too many arguments (got %(argc)s argument(s))
Expected at most %(argc_max)s argument(s))
"""

MSG_INVALID_EMAIL = """
%(option_name)s expects a valid email address (got '%(option_value)s')
"""

MSG_INVALID_JSON_DICT = """
%(option_name)s expects a valid json dictionnary (got '%(option_value)s')
"""

#------------------------------------------------------------------------
# argc 
#------------------------------------------------------------------------

@returns(bool)
#@accepts(StringTypes, int, int)
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
        default_message = MSG_NOT_ENOUGH_ARGS % locals()
    elif argc_max and argc > argc_max:
        default_message = MSG_TOO_MANY_ARGS % locals()
    else:
        invalid_argc = False

    if invalid_argc:
        program_name = sys.argv[0]
        Log.error(error_message % locals())
        sys.exit(CODE_ERROR_BAD_NUM_ARGS)

#------------------------------------------------------------------------
# Generic option 
#------------------------------------------------------------------------

#@accepts(StringTypes, int, function, StringTypes)
def check_option(option_name, option_value, callback_is_valid, message = MSG_INVALID_ARG):
    """
    Check whether an value set for an option belongs to a
    set of values. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        option_name : A String containing the name of the option.
        option_value: A String containing the value set by the user.
            Examples:
                "1"     corresponds to the integer 1
                "'foo'" corresponds to the String "foo"
        callback_is_valid: A function called back which returns True
            if option_value is set to a correct value, False otherwise. 
        message: A format String containing the message to print in case of error.
            Supported format:
                %(option_name)s  : see option_name
                %(option_value)s : see option_value
    """
    if not callback_is_valid(option_value):
        try:
            Log.error(message % locals())
        except ValueError:
            Log.error(format_exc())
            Log.error("Format string not well-formatted: %s" % message)
        sys.exit(CODE_ERROR_INVALID_ARG)

#------------------------------------------------------------------------
# Enum 
#------------------------------------------------------------------------

@accepts(StringTypes, StringTypes, list)
def check_option_enum(option_name, option_value, valid_values):
    """
    Check whether an value set for an option belongs to a
    set of values. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        option_name  : A String containing the name of the option.
        option_value : A String containing the value set by the user.
            Examples:
                "1"     corresponds to the integer 1
                "'foo'" corresponds to the String "foo"
        valid_values : An iterable containing the valid values. 
    """
    if option_value not in valid_values:
        valid_values = ["%s" % value for value in valid_values]
        valid_values = "{%s}" % ", ".join(valid_values)
        try:
            Log.error(MSG_INVALID_ARG_ENUM % locals())
        except ValueError:
            Log.error(format_exc())
            Log.error("Format string not well-formatted: %s" % message)
        sys.exit(CODE_ERROR_INVALID_ARG)

#------------------------------------------------------------------------
# E-mail
#------------------------------------------------------------------------

@returns(bool)
@accepts(StringTypes)
def is_valid_email(email):
    """
    Test whether a String contains a valid email address.
    Args:
        email: A String supposed to contain an email address.
    Returns:
        True iif email is a well-formed email address.
    """
    # See also from email.utils import parseaddr
    import re
    return True if re.match(r"[^@]+@[^@]+\.[^@]+", email) else False

@accepts(StringTypes, StringTypes)
def check_option_email(option_name, option_value):
    """
    Check whether an value set for an option belongs to a
    set of values. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        option_name  : A String containing the name of the option.
        option_value : A String containing the value set by the user.
    """
    check_option(option_name, option_value, is_valid_email, MSG_INVALID_EMAIL)

#------------------------------------------------------------------------
# Bool 
#------------------------------------------------------------------------

STR_TRUE  = ["True",  "TRUE",  "YES", "1"]
STR_FALSE = ["False", "FALSE", "NO",  "0"] 
STR_BOOL  = STR_TRUE + STR_FALSE

@accepts(StringTypes, StringTypes)
def check_option_bool(option_name, option_value):
    """
    Test whether a value is a boolean. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        option_name  : A String containing the name of the option.
        option_value : A String containing the value set by the user.
    """
    check_option_enum(option_name, option_value, STR_BOOL) 

@returns(bool)
@accepts(StringTypes)
def string_to_bool(s):
    """
    Convert a String into a bool.
    Args:
        s: A String supposed to represent a boolean.
    Raises:
        ValueError: if s is not well-formed.
    Returns:
        The boolean corresponding to the input string.
    """
    if s in STR_TRUE:
        return True
    elif s in STR_FALSE: 
        return False
    else:
        raise ValueError("string_to_bool: Invalid string %s (not in %s)" % (s, STR_BOOL))

#------------------------------------------------------------------------
# Json
#------------------------------------------------------------------------

@returns(bool)
@accepts(StringTypes)
def is_valid_json_dict(json_dict):
    """
    Test whether a value is a json dict. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        json_dict: A String supposed to contain an json dict. 
    Returns:
        True iif json_dict is well-formed. 
    """
    ret = True

    try:
        import json
        d = json.loads(json_dict)
    except ImportError, e:
        Log.warning(format_exc())
    except Exception, e:
        Log.error(format_exc())
        ret = False

    return ret

@accepts(StringTypes, StringTypes)
def check_option_json_dict(option_name, option_value):
    """
    Check whether an value set for an option belongs to a
    set of values. Leave the program by returning
    CODE_ERROR_INVALID_ARG in case of failure.
    Args:
        option_name  : A String containing the name of the option.
        option_value : A String containing the value set by the user.
    """
    check_option(option_name, option_value, is_valid_json_dict, MSG_INVALID_JSON_DICT)

#------------------------------------------------------------------------
# Shell wrapping 
#------------------------------------------------------------------------

@returns(int)
#@accepts(StringTypes, list)
def run_command(command, dicts = None):
    """
    Pass a command to a non-interactive Manifold Shell.
    Args:
        command: The command passed to the Manifold Shell 
            Example:
                'SELECT * FROM foo WHERE foo_id == 1'
        dicts: You may either pass None or an empty list.
            - If you pass None, this parameter is ignored
            - If you pass a list, this list is fed with the
              dictionnaries corresponding to each Record
              resulting from the Query corresponding to command.
    Returns:
        CODE_ERROR_PARSING: if the command is not well-formed 
        0                 : iif successful
        >0                : if the command has failed
            See also manifold.core.code
    """
    Shell.init_options()
    shell = Shell(interactive = False)

    try:
        result_value = shell.evaluate(command)
        is_success = result_value.is_success()
        if is_success:
            Log.info(MSG_SUCCESS % locals())
            ret = CODE_SUCCESSFUL
            if isinstance(dicts, list): 
                for record in result_value["value"]:
                    dicts.append(record.to_dict())
        else:
            ret = result_value.get_code() 
    except Exception, e:
        Log.error(format_exc())
        ret = CODE_ERROR_PARSING

    shell.terminate()
    return ret 
