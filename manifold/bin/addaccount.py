#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Enable a platform in the Manifold Storage.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys

from manifold.bin.common    import check_option_email, check_option_enum, check_num_arguments, run_command
from manifold.util.storage  import STORAGE_NAMESPACE 

DOC_ADD_ACCOUNT = """
%(default_message)s

usage: %(program_name)s USER_EMAIL PLATFORM_NAME AUTH_TYPE CONFIG
    Add an Account into the Manifold Storage.

    USER_EMAIL    : A String containing the e-mail address of the user.

    PLATFORM_NAME : A String containing the short name of a platform.
        You can run in 'manifold-shell' the following query to list
        the available platforms:
            SELECT platform FROM local:platform 

    AUTH_TYPE     : A String containing the type of authentification
        presented to the queried platform.
        Supported values are:
            "managed" : let Manifold automatically manage the authentification.
            "user"    : dedicated user credentials.
            "none"    : anonymous authentification.

    CONFIG        : A json encoded dict which may transports additional information
        related to this Account.
        Example:
            '{"firstname" : "John", "lastname" : "Doe", "affiliation" : "Foo"}'
"""

CMD_ADD_ACCOUNT = """
INSERT INTO %(namespace)s:account
    SET user      = "%(user_email)s",
        platform  = "%(platform_name)s",
        auth_type = "%(auth_type)s",
        config    = "%(config)s"
"""

VALID_AUTH_TYPE = ["managed", "user", "none"]

def main():
    check_num_arguments(DOC_ADD_ACCOUNT, 5, 5)
    user_email, platform_name, auth_type, config = sys.argv[1:5]
    check_option_email("USER_EMAIL", user_email)
    check_option_enum("AUTH_TYPE", auth_type, VALID_AUTH_TYPE)
    namespace = STORAGE_NAMESPACE
    return run_command(CMD_ADD_ACCOUNT % locals(), False)

if __name__ == "__main__":
    main()
