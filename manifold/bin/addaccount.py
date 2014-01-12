#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Enable a platform in the Manifold Storage.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys

from manifold.bin.common    import check_num_arguments, run_command
from manifold.util.storage  import STORAGE_NAMESPACE 

DOC_ADD_ACCOUNT = """
%(default_message)s

usage: %(program_name)s USER PLATFORM_NAME AUTH_TYPE CONFIG
    Add an account to Manifold.

    USER          : A String containing the e-mail address of the user.

    PLATFORM_NAME : A String containing the name of the platform.

    AUTH_TYPE     : A String containing the type of authentification
        presented to the queried platform.
        Supported values are:
            "managed" : let Manifold automatically manage the authentification.
            "user"    : dedicated user credentials.
            "none"    : anonymous authentification.

    CONFIG        : A json encoded dict which may transports additional information.
        Example:
            '{"firstname" : "John", "lastname" : "Doe", "affiliation" : "Foo"}'
"""

CMD_ADD_ACCOUNT = """
INSERT INTO %(namespace)s:account
    SET user      = %(user)s,
        platform  = %(platform_name)s,
        auth_type = %(auth_type)s,
        config    = %(config)s
"""

def main():
    check_num_arguments(DOC_ADD_ACCOUNT, 5, 5)
    user, platform_name, auth_type, config = sys.argv[1:5]
    namespace = STORAGE_NAMESPACE
    return run_command(CMD_ADD_ACCOUNT % locals())

if __name__ == '__main__':
    main()
