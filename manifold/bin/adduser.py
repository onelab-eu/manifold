#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Add an user in the Manifold Storage.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import getpass, sys

from manifold.bin.common        import check_option_email, check_num_arguments, run_command
from manifold.util.password     import ask_password

DOC_ADD_USER = """
%(default_message)s

usage: %(program_name)s USER_EMAIL
    Add an User into the Manifold Storage.

    USER_EMAIL : A String containing the e-mail address of the user.
"""

CMD_ADD_USER = """
INSERT INTO %(namespace)s:user
    SET email    = "%(user_email)s",
        password = "%(password)s"
"""

def main():
    check_num_arguments(DOC_ADD_USER, 2, 2)
    user_email = sys.argv[1]
    check_option_email("USER_EMAIL", user_email)
    password = ask_password()
    return run_command(CMD_ADD_USER % locals())

if __name__ == "__main__":
    main()
