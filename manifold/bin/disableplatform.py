#!/usr/bin/env python
#! -*- coding: utf-8 -*-
#
# Disable a platform in the Manifold Storage.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys

from manifold.bin.common    import check_num_arguments, run_command
from manifold.util.storage  import STORAGE_NAMESPACE 

DOC_DISABLE_PLATFORM = """
%(default_message)s

usage: %(program_name)s PLATFORM_NAME

    Disable a platform.

    PLATFORM_NAME: A String containing the short name of a platform managed by Manifold.
        You can run in 'manifold-shell' the following query to list
        the available platforms:
            SELECT platform FROM local:platform 
"""

CMD_DISABLE_PLATFORM = """
UPDATE    %(namespace)s:platform
    SET   disabled = True 
    WHERE platform == "%(platform_name)s"
"""

def main():
    check_num_arguments(DOC_DISABLE_PLATFORM, 2, 2)
    return run_command(CMD_DISABLE_PLATFORM % {
        "platform_name" : sys.argv[1],
        "namespace"     : STORAGE_NAMESPACE
    })

if __name__ == "__main__":
    main()
