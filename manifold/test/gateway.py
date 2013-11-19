#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Functions shared by test-gateway-*.py scripts
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>

import traceback
from types                  import StringTypes

from manifold.bin.shell     import Shell
from manifold.util.log      import Log
from manifold.util.type     import accepts, returns 

MESSAGE_TO_ADD_PLATFORM = "You add '%(platform_name)s' Gateway in the Manifold Storage."

MESSAGE_TO_ENABLE_PLATFORM = """
You must enable '%(platform_name)s' Gateway in the Manifold Storage: please run:

    manifold-enable %(platform_name)s

"""
@accepts(StringTypes)
def display_command(command):
    """
    Display to the standard output a Shell command.
    Args:
        command: A String instance containing a Manifold Query.
    """
    print "=" * 80
    print "%s" % command 
    print "=" * 80

@returns(bool)
@accepts(Shell, StringTypes, StringTypes, StringTypes)
def check_platform(shell, platform_name, message_to_add_platform, message_to_enable_platform):
    """
    Check whether a Platform is referenced and enabled in the Manifold Storage.
    If not, print the appropriate message using the Log class.
    Args:
        platform_name: The platform name 
            ex: "tdmi"
        message_to_add_platform: A String having the format string %(platform_name)s
            ex: "Please add %(platform_name)s in the Manifold Storage"
    Returns:
        True if everything is fine, False otherwise.
    """
    dict_message = {"platform_name" : platform_name}
    ret = False

    result_value = shell.evaluate('SELECT platform, disabled FROM local:platform WHERE platform == "%s"' % platform_name)

    if result_value.is_success():
        records = result_value["value"]
        if records:
            if len(records) > 1:
                Log.warning("Found several Platforms named '%s' in Manifold Storage, this is strange. Selecting the first one..." % platform_name)
            record = records[0]
            if record["disabled"] == True:
                Log.error(MESSAGE_TO_ENABLE_PLATFORM % dict_message)
            else:
                ret = True
        else:
            Log.error(MESSAGE_TO_ADD_TDMI % dict_message)
    else:
        Log.error(result_value.get_error_message())

    return ret

@accepts(Shell, list)
@returns(bool)
def test_commands(shell, commands):
    """
    Test a several Manifold Queries.
    The Shell is terminated if an error occured or once all the commands have been run.
    Args:
        shell: A Shell instance.
        commands: A list of String, each of them containing a manifold-shell command.
    Returns:
        True if all the commands have succeed, False otherwise.
    """
    ret = True
    try:
        for command in commands:
            display_command(command)
            shell.evaluate(command)
            shell.display()
            ret &= shell.get_result_value().is_success()
    except Exception, e:
        Log.error(traceback.format_exc())
        Log.error(e)
        ret = False
    finally:
        shell.terminate()
    return ret

